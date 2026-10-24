import asyncio
import json
import os
import uuid
from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import event as sa_event
from sqlalchemy import select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.infrastructure.database.base import Base, utcnow
from app.infrastructure.database.outbox import Outbox
from app.payments.integrations.webhooks import Webhooks
from app.payments.models.payment import Payment
from app.payments.schemas.payments import PaymentEvent
from app.payments.services.processing import Processor


async def seed(factory):
    async with factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("10.00"),
            currency="USD",
            description="scale test",
            metadata_={},
            idempotency_key=str(uuid.uuid4()),
            request_hash="a" * 64,
            webhook_url="https://example.com/webhook",
        )
        session.add(payment)
        await session.flush()
        return PaymentEvent(payment_id=payment.id, attempt=1)


async def expire(factory, payment_id):
    async with factory() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(lease_expires_at=utcnow() - timedelta(seconds=1))
        )


def processor(factory, gateway=None, webhooks=None):
    if gateway is None:
        gateway = AsyncMock()
        gateway.charge.return_value = "succeeded"
    return Processor(factory, gateway, webhooks or AsyncMock(), Settings())


async def test_no_checked_out_connection_during_gateway_or_webhook(session_factory):
    event = await seed(session_factory)
    engine = session_factory.kw["bind"].sync_engine
    checked_out = 0

    def checkout(*args):
        nonlocal checked_out
        checked_out += 1

    def checkin(*args):
        nonlocal checked_out
        checked_out -= 1

    async def charge(payment):
        assert checked_out == 0
        return "succeeded"

    async def send(payment):
        assert checked_out == 0

    runner = processor(session_factory)
    runner.gateway.charge.side_effect = charge
    runner.webhooks.send.side_effect = send
    sa_event.listen(engine, "checkout", checkout)
    sa_event.listen(engine, "checkin", checkin)
    try:
        await runner.process(event)
        assert checked_out == 0
    finally:
        sa_event.remove(engine, "checkout", checkout)
        sa_event.remove(engine, "checkin", checkin)
    async with session_factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.webhook_delivered_at is not None
        assert payment.lease_token is None


async def test_duplicate_while_claimed_is_ignored_and_restart_recovers(session_factory):
    event = await seed(session_factory)
    old = processor(session_factory)
    assert await old.claim(event) is not None
    restarted = processor(session_factory)
    await restarted.process(event)
    restarted.gateway.charge.assert_not_awaited()
    assert await restarted.recover_expired() == 0
    await expire(session_factory, event.payment_id)
    assert await restarted.recover_expired() == 1
    assert await restarted.recover_expired() == 0
    async with session_factory() as session:
        queued = (await session.scalars(select(Outbox))).one()
        assert queued.payload == event.model_dump(mode="json")
        assert (await session.get(Payment, event.payment_id)).delivery_attempts == 0
    await restarted.process(event)
    restarted.gateway.charge.assert_awaited_once()
    async with session_factory() as session:
        assert (await session.get(Payment, event.payment_id)).webhook_delivered_at is not None


async def test_crash_after_saved_status_does_not_recharge(session_factory):
    event = await seed(session_factory)
    original = processor(session_factory)
    original.webhooks.send.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await original.process(event)
    async with session_factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "succeeded"
        assert payment.lease_token is not None
        assert payment.webhook_delivered_at is None
    await expire(session_factory, event.payment_id)
    restarted = processor(session_factory)
    assert await restarted.recover_expired() == 1
    await restarted.process(event)
    restarted.gateway.charge.assert_not_awaited()
    restarted.webhooks.send.assert_awaited_once()


async def test_stale_gateway_owner_cannot_commit_or_send_webhook(session_factory):
    event = await seed(session_factory)
    old, newer = processor(session_factory), processor(session_factory)
    replacement = None

    async def charge(payment):
        nonlocal replacement
        await expire(session_factory, event.payment_id)
        assert await newer.recover_expired() == 1
        replacement = await newer.claim(event)
        return "succeeded"

    old.gateway.charge.side_effect = charge
    await old.process(event)
    old.webhooks.send.assert_not_awaited()
    async with session_factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "pending"
        assert payment.lease_token == replacement.lease_token
        assert payment.webhook_delivered_at is None


async def test_webhook_commit_crash_repeats_same_stable_event(session_factory):
    event = await seed(session_factory)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200)

    def fail_delivery_commit(session):
        if any(
            isinstance(value, Payment) and value.webhook_delivered_at for value in session.dirty
        ):
            raise SQLAlchemyError("simulated lost completion commit")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        gateway = AsyncMock()
        gateway.charge.return_value = "succeeded"
        sender = Webhooks(client)
        original = processor(session_factory, gateway, sender)
        sa_event.listen(Session, "before_commit", fail_delivery_commit)
        try:
            with pytest.raises(SQLAlchemyError):
                await original.process(event)
        finally:
            sa_event.remove(Session, "before_commit", fail_delivery_commit)
        async with session_factory() as session:
            payment = await session.get(Payment, event.payment_id)
            assert payment.status == "succeeded"
            assert payment.webhook_delivered_at is None
        await expire(session_factory, event.payment_id)
        restarted = processor(session_factory, gateway, sender)
        assert await restarted.recover_expired() == 1
        await restarted.process(event)
    gateway.charge.assert_awaited_once()
    assert len(requests) == 2
    assert requests[0].headers["Idempotency-Key"] == requests[1].headers["Idempotency-Key"]
    assert (
        json.loads(requests[0].content)["event_id"] == json.loads(requests[1].content)["event_id"]
    )
    async with session_factory() as session:
        assert (await session.get(Payment, event.payment_id)).webhook_delivered_at is not None


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"), reason="TEST_DATABASE_URL not configured"
)
async def test_postgres_duplicate_processing_claims_once():
    url = os.environ["TEST_DATABASE_URL"]
    schema = f"test_processing_{uuid.uuid4().hex}"
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        event = await seed(factory)
        gateway, webhooks = AsyncMock(), AsyncMock()
        gateway.charge.return_value = "succeeded"
        runners = [processor(factory, gateway, webhooks) for _ in range(10)]
        await asyncio.gather(*(runner.process(event) for runner in runners))
        gateway.charge.assert_awaited_once()
        webhooks.send.assert_awaited_once()
        async with factory() as session:
            payment = await session.get(Payment, event.payment_id)
            assert payment.delivery_attempts == 1
            assert payment.webhook_delivered_at is not None
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


async def test_final_gateway_failure_survives_crash_before_notification(session_factory):
    event = await seed(session_factory)
    original = processor(session_factory)
    original.gateway.charge.side_effect = TimeoutError("provider unavailable")
    for attempt in (1, 2):
        await original.process(PaymentEvent(payment_id=event.payment_id, attempt=attempt))
    third = PaymentEvent(payment_id=event.payment_id, attempt=3)
    original.webhooks.send.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await original.process(third)
    async with session_factory() as session:
        saved = await session.get(Payment, event.payment_id)
        assert saved.processing_error == "TimeoutError"
        assert saved.status == "failed"
    await expire(session_factory, event.payment_id)
    restarted = processor(session_factory)
    await restarted.recover_expired()
    await restarted.process(third)
    restarted.gateway.charge.assert_not_awaited()
    restarted.webhooks.send.assert_awaited_once()
    async with session_factory() as session:
        saved = await session.get(Payment, event.payment_id)
        assert saved.webhook_delivered_at is not None
        assert saved.dead_lettered_at is not None
        dead = await session.scalar(select(Outbox).where(Outbox.topic == "payments.dlq"))
        assert dead.payload["error_type"] == "TimeoutError"


async def test_gateway_success_before_commit_retries_same_simulated_result(
    session_factory, monkeypatch
):
    from sqlalchemy import inspect

    from app.payments.integrations.gateway import SimulatedGateway

    event = await seed(session_factory)
    monkeypatch.setattr("app.payments.integrations.gateway.random.uniform", lambda *_: 0)
    outcomes = []
    simulator = SimulatedGateway()

    async def charge(payment):
        outcome = await simulator.charge(payment)
        outcomes.append(outcome)
        return outcome

    gateway = AsyncMock()
    gateway.charge.side_effect = charge

    def fail_result_commit(session):
        if any(
            isinstance(value, Payment)
            and value.status != "pending"
            and inspect(value).attrs.status.history.has_changes()
            for value in session.dirty
        ):
            raise SQLAlchemyError("crash after gateway response before result commit")

    original = processor(session_factory, gateway)
    sa_event.listen(Session, "before_commit", fail_result_commit)
    try:
        with pytest.raises(SQLAlchemyError):
            await original.process(event)
    finally:
        sa_event.remove(Session, "before_commit", fail_result_commit)
    async with session_factory() as session:
        assert (await session.get(Payment, event.payment_id)).status == "pending"
    await expire(session_factory, event.payment_id)
    restarted = processor(session_factory, gateway)
    await restarted.recover_expired()
    await restarted.process(event)
    assert len(outcomes) == 2
    assert outcomes[0] == outcomes[1]
    async with session_factory() as session:
        saved = await session.get(Payment, event.payment_id)
        assert saved.status == outcomes[0]
        assert saved.webhook_delivered_at is not None
