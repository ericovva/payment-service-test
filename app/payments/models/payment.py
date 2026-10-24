import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import JSON, CheckConstraint, DateTime, Index, Numeric, String, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.base import Base, utcnow


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        Index(
            "ix_payments_unfinished",
            "id",
            postgresql_where=text("webhook_delivered_at IS NULL AND dead_lettered_at IS NULL"),
        ),
        Index(
            "ix_payments_expired_leases",
            "lease_expires_at",
            postgresql_where=text("lease_token IS NOT NULL"),
        ),
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="valid_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="valid_status"),
        CheckConstraint("delivery_attempts BETWEEN 0 AND 3", name="valid_attempts"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSON)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivery_attempts: Mapped[int] = mapped_column(default=0)
    dead_lettered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    active_attempt: Mapped[int | None]
    processing_error: Mapped[str | None] = mapped_column(String(100))
