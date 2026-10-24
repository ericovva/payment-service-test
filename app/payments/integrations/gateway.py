import asyncio
import hashlib
import random
from typing import Protocol

from app.payments.models.payment import Payment


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> str: ...


class SimulatedGateway:
    async def charge(self, payment: Payment) -> str:
        await asyncio.sleep(random.uniform(2, 5))
        # Stable per payment, like a provider honoring its idempotency key.
        bucket = int.from_bytes(hashlib.sha256(payment.id.bytes).digest()[:8], "big") % 100
        return "succeeded" if bucket < 90 else "failed"
