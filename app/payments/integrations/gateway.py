import asyncio
import random
from typing import Protocol

from app.payments.models.payment import Payment


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> str: ...


class SimulatedGateway:
    async def charge(self, payment: Payment) -> str:
        await asyncio.sleep(random.uniform(2, 5))
        return "succeeded" if random.random() < 0.9 else "failed"
