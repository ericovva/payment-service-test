"""Add processing and outbox leases with active indexes"""

import sqlalchemy as sa
from alembic import op

revision = "3296b87c1f53"
down_revision = "f61a1997f7ee"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("outbox", sa.Column("lease_token", sa.Uuid(), nullable=True))
    op.add_column("outbox", sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True))
    op.drop_index(op.f("ix_outbox_pending"), table_name="outbox")
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["available_at", "locked_until"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
        sqlite_where=sa.text("published_at IS NULL"),
    )
    op.add_column("payments", sa.Column("lease_token", sa.Uuid(), nullable=True))
    op.add_column(
        "payments", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("payments", sa.Column("processing_error", sa.String(length=100), nullable=True))
    op.add_column("payments", sa.Column("active_attempt", sa.Integer(), nullable=True))
    op.create_index(
        "ix_payments_expired_leases",
        "payments",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("lease_token IS NOT NULL"),
    )
    op.create_index(
        "ix_payments_unfinished",
        "payments",
        ["id"],
        unique=False,
        postgresql_where=sa.text("webhook_delivered_at IS NULL AND dead_lettered_at IS NULL"),
    )


def downgrade():
    op.drop_index(
        "ix_payments_unfinished",
        table_name="payments",
        postgresql_where=sa.text("webhook_delivered_at IS NULL AND dead_lettered_at IS NULL"),
    )
    op.drop_index(
        "ix_payments_expired_leases",
        table_name="payments",
        postgresql_where=sa.text("lease_token IS NOT NULL"),
    )
    op.drop_column("payments", "processing_error")
    op.drop_column("payments", "active_attempt")
    op.drop_column("payments", "lease_expires_at")
    op.drop_column("payments", "lease_token")
    op.drop_index(
        "ix_outbox_pending",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
        sqlite_where=sa.text("published_at IS NULL"),
    )
    op.create_index(
        op.f("ix_outbox_pending"), "outbox", ["published_at", "available_at"], unique=False
    )
    op.drop_column("outbox", "locked_until")
    op.drop_column("outbox", "lease_token")
