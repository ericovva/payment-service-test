import asyncio
import logging

from sqlalchemy import select

from app.models import Outbox, utcnow

log = logging.getLogger(__name__)


async def publish_one(sessions, broker) -> bool:
    async with sessions() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= utcnow())
            .order_by(Outbox.available_at, Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        # Only mark published after a persistent, mandatory publish has been confirmed.
        await broker.publish(
            event.payload,
            queue=event.topic,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            timeout=10,
        )
        event.published_at = utcnow()
        return True


async def relay(sessions, broker, interval: float) -> None:
    while True:
        try:
            if await publish_one(sessions, broker):
                continue
        except Exception:
            log.exception("Outbox publication failed; event remains pending")
        await asyncio.sleep(interval)
