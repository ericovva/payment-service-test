import asyncio
import os
import uuid
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infrastructure.database.base import Base, utcnow
from app.infrastructure.database.outbox import Outbox
from app.infrastructure.messaging.outbox import claim_batch, finish_claim, publish_batch


async def seed(factory, count):
    async with factory() as session, session.begin():
        session.add_all([Outbox(topic="payments.new", payload={"number": i}) for i in range(count)])


async def test_claims_bounded_exclusive_and_expired_reclaim(session_factory):
    await seed(session_factory, 3)
    first = await claim_batch(session_factory, 2)
    second = await claim_batch(session_factory, 2)
    assert len(first) == 2 and len(second) == 1
    assert {event.id for event in first}.isdisjoint(event.id for event in second)
    assert not await claim_batch(session_factory)
    async with session_factory() as session, session.begin():
        await session.execute(update(Outbox).values(locked_until=utcnow() - timedelta(seconds=1)))
    reclaimed = await claim_batch(session_factory)
    assert len(reclaimed) == 3
    assert reclaimed[0].token != first[0].token
    assert not await finish_claim(session_factory, first[0], published=True)
    assert not await finish_claim(session_factory, first[0], published=False)
    assert await finish_claim(session_factory, reclaimed[0], published=True)


async def test_publication_failure_releases_with_backoff(session_factory):
    await seed(session_factory, 2)
    broker = AsyncMock()
    broker.publish.side_effect = RuntimeError("Broker unavailable")
    assert await publish_batch(session_factory, broker, retry_seconds=30) == 2
    assert not await claim_batch(session_factory)
    async with session_factory() as session:
        events = (await session.scalars(select(Outbox))).all()
        assert all(event.published_at is None and event.lease_token is None for event in events)
        assert all(event.locked_until is None for event in events)
    # Advance the scheduling state deterministically instead of sleeping.
    async with session_factory() as session, session.begin():
        await session.execute(update(Outbox).values(available_at=utcnow() - timedelta(seconds=1)))
    broker.publish.side_effect = None
    assert await publish_batch(session_factory, broker) == 2
    assert not await claim_batch(session_factory)
    async with session_factory() as session:
        assert all(event.published_at for event in (await session.scalars(select(Outbox))).all())


async def test_publish_is_outside_transaction_and_concurrency_bounded(session_factory):
    await seed(session_factory, 5)
    active = peak = 0
    broker = AsyncMock()

    async def publish(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        # Another write transaction can complete while broker I/O is pending.
        # SQLite would block this if the claim transaction were held open.
        async with session_factory() as session, session.begin():
            await session.execute(update(Outbox).where(Outbox.id == uuid.uuid4()).values(topic="x"))
        assert kwargs["persist"] and kwargs["mandatory"]
        assert kwargs["message_id"]
        await asyncio.sleep(0.01)
        active -= 1

    broker.publish.side_effect = publish
    assert await publish_batch(session_factory, broker, concurrency=2) == 5
    assert peak == 2
    assert not await claim_batch(session_factory)


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"), reason="TEST_DATABASE_URL not configured"
)
async def test_postgres_concurrent_batch_claims_do_not_overlap():
    url = os.environ["TEST_DATABASE_URL"]
    schema = f"test_outbox_{uuid.uuid4().hex}"
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await seed(factory, 100)
        batches = await asyncio.gather(*(claim_batch(factory, 25) for _ in range(4)))
        ids = [event.id for batch in batches for event in batch]
        assert len(ids) == len(set(ids)) == 100
        assert not await claim_batch(factory)
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


async def test_renewal_only_extends_current_unpublished_claims(session_factory, monkeypatch):
    from app.infrastructure.messaging import outbox as module

    await seed(session_factory, 3)
    claimed = await claim_batch(session_factory, lease_seconds=1)
    await finish_claim(session_factory, claimed[0], published=True)
    newer_token = uuid.uuid4()
    async with session_factory() as session, session.begin():
        await session.execute(
            update(Outbox)
            .where(Outbox.id == claimed[1].id)
            .values(
                lease_token=newer_token,
            )
        )
    fixed_now = utcnow() + timedelta(seconds=5)
    monkeypatch.setattr(module, "utcnow", lambda: fixed_now)
    calls = 0

    async def tick(delay):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(module.asyncio, "sleep", tick)
    with pytest.raises(asyncio.CancelledError):
        await module._renew_claims(session_factory, claimed, 30)
    async with session_factory() as session:
        done = await session.get(Outbox, claimed[0].id)
        replaced = await session.get(Outbox, claimed[1].id)
        renewed = await session.get(Outbox, claimed[2].id)
        assert done.locked_until is None
        assert replaced.lease_token == newer_token
        assert replaced.locked_until.replace(tzinfo=fixed_now.tzinfo) < fixed_now
        assert renewed.locked_until.replace(tzinfo=fixed_now.tzinfo) == (
            fixed_now + timedelta(seconds=30)
        )


async def test_batch_completion_uses_one_commit_and_splits_failure(session_factory):
    from sqlalchemy import event as sa_event
    from sqlalchemy.orm import Session

    await seed(session_factory, 8)
    commits = 0

    def committed(session):
        nonlocal commits
        commits += 1

    async def publish(payload, **kwargs):
        if payload["number"] % 2:
            raise ConnectionError("unavailable")

    broker = AsyncMock()
    broker.publish.side_effect = publish
    sa_event.listen(Session, "after_commit", committed)
    try:
        assert await publish_batch(session_factory, broker) == 8
    finally:
        sa_event.remove(Session, "after_commit", committed)
    assert commits == 2  # One batch claim, one batch completion (not eight completions).
    async with session_factory() as session:
        events = (await session.scalars(select(Outbox))).all()
        assert sum(event.published_at is not None for event in events) == 4
        assert all(event.lease_token is None for event in events)
        assert all(
            event.published_at is not None or event.available_at > event.created_at
            for event in events
        )
