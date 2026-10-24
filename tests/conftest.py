"""Fast tests use a disposable SQLite database and never the deployment DB."""

import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Set before app imports: Settings and the module-level engine are initialized on import.
os.environ["API_KEY"] = "test-key-at-least-sixteen"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["WEBHOOK_ALLOWED_HOSTS"] = '["example.com"]'
os.environ["WEBHOOK_ALLOW_HTTP"] = "false"

from app.api import app  # noqa: E402
from app.db import get_session  # noqa: E402
from app.models import Base  # noqa: E402


@pytest.fixture
async def session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'test.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.fixture
async def client(session_factory):
    async def override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": "test-key-at-least-sixteen"},
        ) as http:
            yield http
    finally:
        app.dependency_overrides.pop(get_session, None)


@pytest.fixture
def payment_body():
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Test purchase",
        "metadata": {"order_id": "order-1"},
        "webhook_url": "https://example.com/webhook",
    }
