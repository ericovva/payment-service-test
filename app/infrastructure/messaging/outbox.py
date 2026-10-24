import asyncio
import logging
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import or_, select, update

from app.infrastructure.database.base import utcnow
from app.infrastructure.database.outbox import Outbox

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimedEvent:
    id: uuid.UUID
    topic: str
    payload: dict[str, Any]
    token: uuid.UUID


async def claim_batch(sessions, batch_size: int = 100, lease_seconds: float = 30):
    """Claim in a short transaction; no broker I/O holds database locks."""
    if batch_size < 1 or lease_seconds <= 0:
        raise ValueError("Batch size and lease duration must be positive")
    now, token = utcnow(), uuid.uuid4()
    async with sessions() as session, session.begin():
        events = (
            await session.scalars(
                select(Outbox)
                .where(
                    Outbox.published_at.is_(None),
                    Outbox.available_at <= now,
                    or_(Outbox.locked_until.is_(None), Outbox.locked_until <= now),
                )
                .order_by(Outbox.available_at, Outbox.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        claimed = []
        for event in events:
            event.lease_token = token
            event.locked_until = now + timedelta(seconds=lease_seconds)
            claimed.append(ClaimedEvent(event.id, event.topic, event.payload, token))
    return claimed


async def finish_claim(
    sessions, event: ClaimedEvent, *, published: bool, retry_seconds: float = 1
) -> bool:
    """A stale publisher must never clear or complete a newer publisher's claim."""
    now = utcnow()
    values = {"lease_token": None, "locked_until": None}
    if published:
        values["published_at"] = now
    else:
        values["available_at"] = now + timedelta(seconds=retry_seconds)
    async with sessions() as session, session.begin():
        result = await session.execute(
            update(Outbox)
            .where(
                Outbox.id == event.id,
                Outbox.lease_token == event.token,
                Outbox.published_at.is_(None),
            )
            .values(**values)
        )
        return result.rowcount == 1


async def _publish_claim(sessions, broker, event, retry_seconds):
    try:
        await broker.publish(
            event.payload,
            queue=event.topic,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            timeout=10,
        )
    except Exception:
        await finish_claim(sessions, event, published=False, retry_seconds=retry_seconds)
        raise
    await finish_claim(sessions, event, published=True)


async def publish_one(sessions, broker) -> bool:
    """Compatibility helper; production relay uses bounded batches with backoff."""
    events = await claim_batch(sessions, batch_size=1)
    if not events:
        return False
    await _publish_claim(sessions, broker, events[0], retry_seconds=0)
    return True


async def _renew_claims(sessions, events, lease_seconds):
    while True:
        await asyncio.sleep(lease_seconds / 3)
        async with sessions() as session, session.begin():
            await session.execute(
                update(Outbox)
                .where(Outbox.lease_token == events[0].token, Outbox.published_at.is_(None))
                .values(locked_until=utcnow() + timedelta(seconds=lease_seconds))
            )


async def publish_batch(
    sessions, broker, batch_size=100, concurrency=10, lease_seconds=30, retry_seconds=1
) -> int:
    if concurrency < 1 or retry_seconds <= 0:
        raise ValueError("Concurrency and retry delay must be positive")
    events = await claim_batch(sessions, batch_size, lease_seconds)
    if not events:
        return 0
    semaphore = asyncio.Semaphore(concurrency)

    async def publish(event):
        async with semaphore:
            try:
                await broker.publish(
                    event.payload,
                    queue=event.topic,
                    persist=True,
                    mandatory=True,
                    message_id=str(event.id),
                    timeout=10,
                )
            except Exception:
                log.exception("Outbox publication failed; event remains pending")
                return event.id, False
            return event.id, True

    renewal = asyncio.create_task(_renew_claims(sessions, events, lease_seconds))
    try:
        results = await asyncio.gather(*(publish(event) for event in events))
        succeeded = [event_id for event_id, success in results if success]
        failed = [event_id for event_id, success in results if not success]
        now = utcnow()
        # One commit per batch, not one connection/commit per broker message.
        # A crash before this commit permits duplicate publication, never data loss.
        async with sessions() as session, session.begin():
            owned = update(Outbox).where(
                Outbox.lease_token == events[0].token,
                Outbox.published_at.is_(None),
            )
            if succeeded:
                await session.execute(
                    owned.where(Outbox.id.in_(succeeded)).values(
                        published_at=now,
                        lease_token=None,
                        locked_until=None,
                    )
                )
            if failed:
                await session.execute(
                    owned.where(Outbox.id.in_(failed)).values(
                        available_at=now + timedelta(seconds=retry_seconds),
                        lease_token=None,
                        locked_until=None,
                    )
                )
    finally:
        renewal.cancel()
        with suppress(asyncio.CancelledError):
            await renewal
    return len(events)


async def relay(
    sessions,
    broker,
    interval: float,
    batch_size=100,
    concurrency=10,
    lease_seconds=30,
    retry_seconds=1,
) -> None:
    while True:
        try:
            if await publish_batch(
                sessions, broker, batch_size, concurrency, lease_seconds, retry_seconds
            ):
                continue
        except Exception:
            log.exception("Outbox relay failed; expired leases will be reclaimed")
        await asyncio.sleep(interval)
