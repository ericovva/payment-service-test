import json
import uuid
from datetime import UTC
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.infrastructure.database.base import utcnow
from app.infrastructure.database.outbox import Outbox
from app.infrastructure.messaging.outbox import publish_one
from app.payments.integrations.webhooks import Webhooks
from app.payments.models.payment import Payment
from app.payments.schemas.payments import PaymentEvent
from app.payments.services.processing import Processor


async def seed_payment(sessions):
    async with sessions() as session, session.begin():
        payment = Payment(
            amount=Decimal("12.50"),
            currency="RUB",
            description="test",
            metadata_={},
            idempotency_key="test-key",
            request_hash="a" * 64,
            webhook_url="https://example.com/webhook",
        )
        session.add(payment)
        await session.flush()
        return payment.id


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_process_success_and_duplicate(session_factory, status):
    payment_id = await seed_payment(session_factory)
    gateway = AsyncMock()
    gateway.charge.return_value = status
    webhooks = AsyncMock()
    processor = Processor(session_factory, gateway, webhooks, Settings())
    event = PaymentEvent(payment_id=payment_id, attempt=1)
    await processor.process(event)
    await processor.process(event)
    gateway.charge.assert_awaited_once()
    webhooks.send.assert_awaited_once()
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == status
        assert payment.webhook_delivered_at is not None
        assert payment.processed_at is not None
        assert payment.delivery_attempts == 1
        assert not (await session.scalars(select(Outbox))).all()


async def test_webhook_retries_persist_and_dlq_without_recharging(session_factory):
    payment_id = await seed_payment(session_factory)
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    webhooks = AsyncMock()
    webhooks.send.side_effect = httpx.ConnectError("offline")
    for attempt in range(1, 4):
        before = utcnow()
        # Recreating the processor simulates losing process-local state on restart.
        processor = Processor(session_factory, gateway, webhooks, Settings())
        await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
        async with session_factory() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.delivery_attempts == attempt
            assert payment.status == "succeeded"
            assert payment.webhook_delivered_at is None
            rows = (await session.scalars(select(Outbox).order_by(Outbox.created_at))).all()
            assert len(rows) == attempt
            row = rows[-1]
            if attempt < 3:
                assert row.topic == "payments.new"
                assert row.payload["attempt"] == attempt + 1
                # SQLite strips time zones; compare in UTC explicitly.
                assert row.available_at.replace(tzinfo=UTC) >= before.replace(tzinfo=UTC)
                delay = (row.available_at - row.created_at).total_seconds()
                assert 2 ** (attempt - 1) - 0.1 <= delay <= 2 ** (attempt - 1) + 0.1
            else:
                assert row.topic == "payments.dlq"
                assert payment.dead_lettered_at is not None
        # Redelivered old attempt is ignored, including after the terminal DLQ decision.
        await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
    gateway.charge.assert_awaited_once()
    assert webhooks.send.await_count == 3


async def test_gateway_exception_gets_three_attempts(session_factory):
    payment_id = await seed_payment(session_factory)
    gateway = AsyncMock()
    gateway.charge.side_effect = TimeoutError("gateway unavailable")
    webhooks = AsyncMock()
    processor = Processor(session_factory, gateway, webhooks, Settings())
    for attempt in range(1, 4):
        await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
    assert gateway.charge.await_count == 3
    webhooks.send.assert_not_awaited()
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "failed"
        assert payment.dead_lettered_at is not None
        assert payment.processed_at is not None
        assert len((await session.scalars(select(Outbox))).all()) == 3


async def test_webhook_retry_can_recover(session_factory):
    payment_id = await seed_payment(session_factory)
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    webhooks = AsyncMock()
    webhooks.send.side_effect = [TimeoutError(), None]
    processor = Processor(session_factory, gateway, webhooks, Settings())
    await processor.process(PaymentEvent(payment_id=payment_id, attempt=1))
    await processor.process(PaymentEvent(payment_id=payment_id, attempt=2))
    gateway.charge.assert_awaited_once()
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.webhook_delivered_at is not None
        assert payment.delivery_attempts == 2
        assert payment.dead_lettered_at is None


async def test_outbox_failure_then_confirmed_publish(session_factory):
    async with session_factory() as session, session.begin():
        event = Outbox(topic="payments.new", payload={"test": True})
        session.add(event)
        await session.flush()
        event_id = event.id
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("broker offline")
    with pytest.raises(ConnectionError):
        await publish_one(session_factory, broker)
    async with session_factory() as session:
        assert (await session.get(Outbox, event_id)).published_at is None
    broker.publish.side_effect = None
    assert await publish_one(session_factory, broker)
    broker.publish.assert_awaited_with(
        {"test": True},
        queue="payments.new",
        persist=True,
        mandatory=True,
        message_id=str(event_id),
        timeout=10,
    )
    async with session_factory() as session:
        assert (await session.get(Outbox, event_id)).published_at is not None
    assert not await publish_one(session_factory, broker)


async def test_webhook_stable_event_id_and_no_redirect():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": "https://internal.example/secret"})

    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("12.50"),
        currency="RUB",
        status="succeeded",
        metadata_={"order": 1},
        processed_at=utcnow(),
        webhook_url="https://example.com/webhook",
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
    ) as client:
        sender = Webhooks(client)
        for _ in range(2):
            with pytest.raises(httpx.HTTPStatusError):
                await sender.send(payment)
    assert len(requests) == 2
    event_id = f"payment.{payment.id}.completed"
    for request in requests:
        assert request.url.host == "example.com"
        assert request.headers["Idempotency-Key"] == event_id
        assert request.headers["X-Event-ID"] == event_id
        assert json.loads(request.content)["event_id"] == event_id


async def test_outbox_waits_until_retry_is_due(session_factory):
    from datetime import timedelta

    async with session_factory() as session, session.begin():
        session.add(
            Outbox(
                topic="payments.new",
                payload={"attempt": 2},
                available_at=utcnow() + timedelta(hours=1),
            )
        )
    broker = AsyncMock()
    assert not await publish_one(session_factory, broker)
    broker.publish.assert_not_awaited()
