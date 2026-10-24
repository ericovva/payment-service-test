import asyncio
import os
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infrastructure.database.base import Base
from app.infrastructure.database.outbox import Outbox
from app.infrastructure.database.session import get_session
from app.main import app
from app.payments.models.payment import Payment

PAYMENTS = "/api/v1/payments"


@pytest.mark.parametrize("path", ["/health", "/openapi.json", "/unknown", PAYMENTS])
@pytest.mark.parametrize("key", [None, "wrong-key"])
async def test_authentication_covers_every_route(client, path, key):
    headers = {} if key is None else {"X-API-Key": key}
    # A separate client avoids inheriting the authenticated fixture's default header.
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        response = await http.get(path, headers=headers)
        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid API key"}
        response = await http.post(path, headers=headers, json={})
        assert response.status_code == 401


async def test_authenticated_health_schema_and_unknown_route(client):
    assert (await client.get("/health")).json() == {"status": "ok"}
    schema = await client.get("/openapi.json")
    assert schema.status_code == 200
    assert PAYMENTS in schema.json()["paths"]
    assert (await client.get("/unknown")).status_code == 404


async def test_create_get_and_transactional_outbox(client, session_factory, payment_body):
    response = await client.post(
        PAYMENTS,
        json=payment_body,
        headers={"Idempotency-Key": "order-1"},
    )
    assert response.status_code == 202, response.text
    accepted = response.json()
    payment_id = uuid.UUID(accepted["payment_id"])
    assert accepted["status"] == "pending"
    assert accepted["created_at"]
    response = await client.get(f"{PAYMENTS}/{payment_id}")
    assert response.status_code == 200
    payment = response.json()
    for key in ("amount", "currency", "description", "metadata", "webhook_url"):
        assert payment[key] == payment_body[key]
    assert payment["status"] == "pending"
    assert payment["delivery_attempts"] == 0
    assert payment["processed_at"] is None
    assert payment["webhook_delivered_at"] is None
    assert payment["dead_lettered_at"] is None
    async with session_factory() as session:
        events = (await session.scalars(select(Outbox))).all()
        assert len(events) == 1
        assert events[0].topic == "payments.new"
        assert events[0].payload == {"payment_id": str(payment_id), "attempt": 1}
        assert events[0].published_at is None


@pytest.mark.parametrize(
    "headers",
    [{}, {"Idempotency-Key": ""}, {"Idempotency-Key": "   "}, {"Idempotency-Key": "x" * 256}],
)
async def test_idempotency_key_required_and_bounded(client, payment_body, headers):
    assert (await client.post(PAYMENTS, json=payment_body, headers=headers)).status_code == 422


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "10000000000000000.00"),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("currency", "GBP"),
        ("currency", "usd"),
        ("webhook_url", "not-a-url"),
        ("webhook_url", "https://other.example/webhook"),
        ("webhook_url", "http://example.com/webhook"),
        ("webhook_url", "https://user:password@example.com/webhook"),
        ("webhook_url", "https://example.com/webhook#fragment"),
        ("description", "x" * 1001),
        ("metadata", []),
        ("unexpected", "field"),
    ],
)
async def test_invalid_request_does_not_write(client, session_factory, payment_body, field, value):
    response = await client.post(
        PAYMENTS,
        json={**payment_body, field: value},
        headers={"Idempotency-Key": "invalid"},
    )
    assert response.status_code == 422, response.text
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_equivalent_retries_return_same_payment_once(client, session_factory, payment_body):
    bodies = [{**payment_body, "amount": amount} for amount in ("100", "100.0", "100.00")]
    responses = [
        await client.post(PAYMENTS, json=body, headers={"Idempotency-Key": "same"})
        for body in bodies
    ]
    assert [response.status_code for response in responses] == [202, 202, 202]
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_idempotency_conflict(client, session_factory, payment_body):
    headers = {"Idempotency-Key": "conflict"}
    assert (await client.post(PAYMENTS, json=payment_body, headers=headers)).status_code == 202
    response = await client.post(PAYMENTS, json={**payment_body, "amount": "999"}, headers=headers)
    assert response.status_code == 409
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_payment_not_found_and_invalid_identifier(client):
    assert (await client.get(f"{PAYMENTS}/{uuid.uuid4()}")).status_code == 404
    assert (await client.get(f"{PAYMENTS}/not-a-uuid")).status_code == 422


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not configured",
)
async def test_postgres_concurrent_idempotency(payment_body):
    """Exercise the real PostgreSQL unique-index race in an isolated temporary schema."""
    url = os.environ["TEST_DATABASE_URL"]
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail("TEST_DATABASE_URL must use postgresql+asyncpg://")
    schema = f"test_payments_{uuid.uuid4().hex}"
    admin_engine = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def override_session():
        async with factory() as session:
            yield session

    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        app.dependency_overrides[get_session] = override_session
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": "test-key-at-least-sixteen", "Idempotency-Key": "race"},
        ) as client:
            responses = await asyncio.gather(
                *[client.post(PAYMENTS, json=payment_body) for _ in range(10)]
            )
        assert [response.status_code for response in responses] == [202] * 10
        assert len({response.json()["payment_id"] for response in responses}) == 1
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Payment)) == 1
            assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
    finally:
        app.dependency_overrides.pop(get_session, None)
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()
