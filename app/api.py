import secrets
import uuid
from contextlib import asynccontextmanager
from typing import Annotated
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import engine, get_session
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="Payment service", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan
)


@app.middleware("http")
async def authenticate(request, call_next):
    provided = request.headers.get("X-API-Key", "")
    expected = get_settings().api_key.get_secret_value()
    if not secrets.compare_digest(provided.encode(), expected.encode()):
        return JSONResponse({"detail": "Invalid API key"}, status_code=401)
    return await call_next(request)


Session = Annotated[AsyncSession, Depends(get_session)]


@app.get("/health")
async def health(session: Session):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    return app.openapi()


@app.post("/api/v1/payments", response_model=PaymentAccepted, status_code=202)
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
            raise HTTPException(409, "Idempotency-Key was used with a different request") from None
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentRead)
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
