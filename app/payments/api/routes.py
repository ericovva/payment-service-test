import uuid
from typing import Annotated
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.infrastructure.database.session import get_session
from app.payments.models.payment import Payment
from app.payments.repositories import payments as repository
from app.payments.schemas.payments import PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter()
Session = Annotated[AsyncSession, Depends(get_session)]


@router.post("/api/v1/payments", response_model=PaymentAccepted, status_code=202)
async def create_payment(
    body: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
):
    if not idempotency_key.strip():
        raise HTTPException(422, "Idempotency-Key must not be blank")
    settings = get_settings()
    url = urlsplit(str(body.webhook_url))
    if url.hostname not in settings.webhook_allowed_hosts:
        raise HTTPException(422, "Webhook host is not allowed")
    if url.scheme != "https" and not settings.webhook_allow_http:
        raise HTTPException(422, "Webhook URL must use HTTPS")
    try:
        payment = await repository.create_payment(session, body, idempotency_key)
    except repository.IdempotencyConflict:
        raise HTTPException(409, "Idempotency-Key was used with a different request") from None
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/api/v1/payments/{payment_id}", response_model=PaymentRead)
async def get_payment(payment_id: uuid.UUID, session: Session):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return PaymentRead(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
        delivery_attempts=payment.delivery_attempts,
        dead_lettered_at=payment.dead_lettered_at,
    )
