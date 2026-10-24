import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Index, String, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.base import Base, utcnow


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_pending",
            "available_at",
            "locked_until",
            postgresql_where=text("published_at IS NULL"),
            sqlite_where=text("published_at IS NULL"),
        ),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    topic: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
