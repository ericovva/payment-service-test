import asyncio
import logging
import uuid
from datetime import UTC, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.infrastructure.database.base import utcnow
from app.infrastructure.database.outbox import Outbox
from app.payments.integrations.gateway import Gateway
from app.payments.integrations.webhooks import Webhooks
from app.payments.models.payment import Payment
from app.payments.schemas.payments import PaymentEvent

log = logging.getLogger(__name__)


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

    async def claim(self, event: PaymentEvent) -> Payment | None:
        async with self.sessions() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if payment is None:
                raise ValueError("Unknown payment")
            if not self.active(payment, event) or payment.lease_token is not None:
                return None
            payment.lease_token = uuid.uuid4()
            payment.active_attempt = event.attempt
            payment.lease_expires_at = utcnow() + timedelta(
                seconds=self.settings.processing_lease_seconds
            )
            return payment

    @staticmethod
    def release(payment: Payment) -> None:
        payment.lease_token = None
        payment.lease_expires_at = None
        payment.active_attempt = None

    def failed(
        self, session: AsyncSession, payment: Payment, event: PaymentEvent, error: Exception | str
    ) -> None:
        error_type = error if isinstance(error, str) else type(error).__name__
        payment.delivery_attempts = event.attempt
        self.release(payment)
        if event.attempt == 3:
            payment.dead_lettered_at = utcnow()
            topic, delay = "payments.dlq", 0
            payload = event.model_dump(mode="json") | {"error_type": error_type}
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
                "error_type": error_type,
            },
        )

    async def owned(self, session: AsyncSession, payment: Payment) -> Payment | None:
        return await session.scalar(
            select(Payment)
            .where(Payment.id == payment.id, Payment.lease_token == payment.lease_token)
            .with_for_update()
        )

    async def process(self, event: PaymentEvent) -> None:
        payment = await self.claim(event)
        if payment is None:
            return
        gateway_error = payment.processing_error
        if payment.status == "pending":
            try:
                async with asyncio.timeout(self.settings.gateway_timeout):
                    status = await self.gateway.charge(payment)
            except Exception as error:
                if event.attempt < 3:
                    async with self.sessions() as session, session.begin():
                        current = await self.owned(session, payment)
                        if current:
                            self.failed(session, current, event, error)
                    return
                status, gateway_error = "failed", type(error).__name__
            # Fence stale owners and persist the result before sending any webhook.
            async with self.sessions() as session, session.begin():
                current = await self.owned(session, payment)
                if current is None:
                    return
                current.status, current.processed_at = status, utcnow()
                current.processing_error = gateway_error
                payment.status, payment.processed_at = current.status, current.processed_at
        # Never begin network work after our ownership window has expired.
        deadline = payment.lease_expires_at.replace(tzinfo=UTC)
        remaining = (deadline - utcnow()).total_seconds()
        if remaining <= 0:
            return
        webhook_error = None
        try:
            async with asyncio.timeout(min(self.settings.webhook_timeout, remaining)):
                await self.webhooks.send(payment)
        except Exception as error:
            webhook_error = error
        async with self.sessions() as session, session.begin():
            current = await self.owned(session, payment)
            if current is None:
                return
            if webhook_error:
                self.failed(session, current, event, webhook_error)
            elif gateway_error:
                current.webhook_delivered_at = utcnow()
                self.failed(session, current, event, gateway_error)
            else:
                current.delivery_attempts = event.attempt
                current.webhook_delivered_at = utcnow()
                self.release(current)

    async def recover_expired(self, batch_size: int = 100) -> int:
        async with self.sessions() as session, session.begin():
            payments = (
                await session.scalars(
                    select(Payment)
                    .where(Payment.lease_token.is_not(None), Payment.lease_expires_at <= utcnow())
                    .order_by(Payment.lease_expires_at)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for payment in payments:
                session.add(
                    Outbox(
                        topic="payments.new",
                        payload={"payment_id": str(payment.id), "attempt": payment.active_attempt},
                    )
                )
                self.release(payment)
            return len(payments)

    async def recovery_loop(self) -> None:
        while True:
            try:
                if await self.recover_expired():
                    continue
            except Exception:
                log.exception("Expired payment lease recovery failed")
            await asyncio.sleep(1)
