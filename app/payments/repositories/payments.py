from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.database.outbox import Outbox
from app.payments.models.payment import Payment
from app.payments.schemas.payments import PaymentCreate


class BacklogFull(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession, body: PaymentCreate, idempotency_key: str, admit=None
) -> Payment:
    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata_=body.metadata,
        webhook_url=str(body.webhook_url),
        idempotency_key=idempotency_key,
        request_hash=body.fingerprint(),
    )
    try:
        async with session.begin():
            existing = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if existing:
                if existing.request_hash != body.fingerprint():
                    raise IdempotencyConflict
                return existing
            if admit is not None and not admit():
                raise BacklogFull
            session.add(payment)
            await session.flush()
            session.add(
                Outbox(topic="payments.new", payload={"payment_id": str(payment.id), "attempt": 1})
            )
    except IntegrityError:
        # The unique index arbitrates concurrent requests, not an unsafe pre-insert lookup.
        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if payment is None:
            raise
        if payment.request_hash != body.fingerprint():
            raise IdempotencyConflict from None
    return payment
