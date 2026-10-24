import asyncio
import logging
import time

from sqlalchemy import func, select

from app.infrastructure.database.outbox import Outbox
from app.payments.models.payment import Payment

log = logging.getLogger(__name__)


class BacklogGate:
    """A cached, cluster-wide backlog signal, not a globally serialized hard quota."""

    def __init__(self, sessions, limit: int, refresh_seconds: float = 1):
        self.sessions = sessions
        self.limit = limit
        self.refresh_seconds = refresh_seconds
        self.pending_payments = 0
        self.pending_outbox = 0
        self.refreshed_at = 0.0

    def allows_new_payment(self) -> bool:
        if not self.limit:
            return True
        fresh = time.monotonic() - self.refreshed_at <= self.refresh_seconds * 3
        return fresh and self.pending_payments < self.limit

    async def refresh(self):
        async with self.sessions() as session:
            self.pending_payments = await session.scalar(
                select(func.count())
                .select_from(Payment)
                .where(Payment.webhook_delivered_at.is_(None), Payment.dead_lettered_at.is_(None))
            )
            self.pending_outbox = await session.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
            )
        self.refreshed_at = time.monotonic()

    async def run(self):
        while True:
            try:
                await self.refresh()
            except Exception:
                log.exception("Backlog refresh failed; stale admission closes to new payments")
            await asyncio.sleep(self.refresh_seconds)

    def snapshot(self):
        return {
            "pending_payments": self.pending_payments,
            "pending_outbox": self.pending_outbox,
            "limit": self.limit,
            "accepting_new_payments": self.allows_new_payment(),
            "snapshot_age_seconds": round(time.monotonic() - self.refreshed_at, 3),
        }
