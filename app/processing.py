import asyncio
import logging
import random
from datetime import timedelta
from typing import Protocol

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import Outbox, Payment, utcnow
from app.schemas import PaymentEvent

log = logging.getLogger(__name__)


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> str: ...


class SimulatedGateway:
    async def charge(self, payment: Payment) -> str:
        await asyncio.sleep(random.uniform(2, 5))
        return "succeeded" if random.random() < 0.9 else "failed"


class Webhooks:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def send(self, payment: Payment) -> None:
        event_id = f"payment.{payment.id}.completed"
        response = await self.client.post(
            payment.webhook_url,
            headers={"Idempotency-Key": event_id, "X-Event-ID": event_id},
            json={
                "event_id": event_id,
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "metadata": payment.metadata_,
                "processed_at": payment.processed_at.isoformat(),
            },
        )
        response.raise_for_status()


class Processor:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        gateway: Gateway,
        webhooks: Webhooks,
        settings: Settings,
    ):
        self.sessions = sessions
        self.gateway = gateway
        self.webhooks = webhooks
        self.settings = settings

    @staticmethod
    def active(payment: Payment, event: PaymentEvent) -> bool:
        return (
            not payment.webhook_delivered_at
            and not payment.dead_lettered_at
            and event.attempt == payment.delivery_attempts + 1
        )

    def failed(
        self, session: AsyncSession, payment: Payment, event: PaymentEvent, error: Exception
    ) -> None:
        payment.delivery_attempts = event.attempt
        if event.attempt == 3:
            payment.dead_lettered_at = utcnow()
            # An unavailable gateway is not a successful charge.
            if payment.status == "pending":
                payment.status = "failed"
                payment.processed_at = utcnow()
            topic, delay = "payments.dlq", 0
            payload = event.model_dump(mode="json") | {"error_type": type(error).__name__}
        else:
            topic = "payments.new"
            delay = self.settings.retry_base_seconds * 2 ** (event.attempt - 1)
            payload = {"payment_id": str(payment.id), "attempt": event.attempt + 1}
        session.add(
            Outbox(topic=topic, payload=payload, available_at=utcnow() + timedelta(seconds=delay))
        )
        log.warning(
            "Payment attempt failed",
            extra={
                "payment_id": str(payment.id),
                "attempt": event.attempt,
                "error_type": type(error).__name__,
            },
        )

    async def process(self, event: PaymentEvent) -> None:
        async with self.sessions() as session:
            # Commit the gateway result before attempting the webhook.
            async with session.begin():
                payment = await session.scalar(
                    select(Payment).where(Payment.id == event.payment_id).with_for_update()
                )
                if payment is None:
                    raise ValueError("Unknown payment")
                if not self.active(payment, event):
                    return
                if payment.status == "pending":
                    try:
                        payment.status = await self.gateway.charge(payment)
                        payment.processed_at = utcnow()
                    except Exception as error:
                        self.failed(session, payment, event, error)
                        return
            async with session.begin():
                payment = await session.scalar(
                    select(Payment)
                    .where(Payment.id == event.payment_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if not self.active(payment, event):
                    return
                try:
                    await self.webhooks.send(payment)
                except Exception as error:
                    self.failed(session, payment, event, error)
                else:
                    payment.delivery_attempts = event.attempt
                    payment.webhook_delivered_at = utcnow()
