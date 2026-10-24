import asyncio
import time
import uuid
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.admission import BacklogGate
from app.core.limits import RequestLimits
from app.infrastructure.database.base import utcnow
from app.infrastructure.database.outbox import Outbox
from app.main import app
from app.payments.models.payment import Payment


def test_gate_requires_fresh_signal_and_rejects_full_or_stale():
    gate = BacklogGate(None, limit=2, refresh_seconds=1)
    assert not gate.allows_new_payment()
    gate.refreshed_at = time.monotonic()
    assert gate.allows_new_payment()
    gate.pending_payments = 2
    assert not gate.allows_new_payment()
    gate.pending_payments = 0
    gate.refreshed_at -= 4
    assert not gate.allows_new_payment()
    gate.limit = 0
    assert gate.allows_new_payment()


async def test_refresh_excludes_delivered_and_dead_lettered(session_factory):
    async with session_factory() as session, session.begin():
        for kind in ("pending", "delivered", "dead"):
            session.add(
                Payment(
                    amount=Decimal("1"),
                    currency="USD",
                    description="",
                    metadata_={},
                    idempotency_key=kind,
                    request_hash="a" * 64,
                    webhook_url="https://example.com/webhook",
                    webhook_delivered_at=utcnow() if kind == "delivered" else None,
                    dead_lettered_at=utcnow() if kind == "dead" else None,
                )
            )
        session.add(Outbox(topic="payments.new", payload={}))
        session.add(Outbox(topic="payments.new", payload={}, published_at=utcnow()))
    gate = BacklogGate(session_factory, limit=2)
    await gate.refresh()
    assert gate.pending_payments == 1
    assert gate.pending_outbox == 1
    assert gate.allows_new_payment()
    assert gate.snapshot()["snapshot_age_seconds"] < 1


async def test_full_gate_replays_existing_key_but_rejects_new(
    client,
    session_factory,
    payment_body,
    monkeypatch,
):
    gate = BacklogGate(session_factory, limit=1)
    await gate.refresh()
    monkeypatch.setattr(app.state, "backlog", gate)
    headers = {"Idempotency-Key": "already-created"}
    first = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert first.status_code == 202
    await gate.refresh()
    assert not gate.allows_new_payment()
    replay = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert replay.status_code == 202
    assert replay.json()["payment_id"] == first.json()["payment_id"]
    conflict = await client.post(
        "/api/v1/payments", json={**payment_body, "amount": "1"}, headers=headers
    )
    assert conflict.status_code == 409
    rejected = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "new"}
    )
    assert rejected.status_code == 429
    assert rejected.headers["Retry-After"] == "1"
    gate.refreshed_at -= 4
    assert (
        await client.post(
            "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "stale"}
        )
    ).status_code == 429


@pytest.mark.parametrize("chunked", [False, True])
async def test_body_larger_than_64k_is_rejected_before_json_parse(client, chunked):
    async def chunks():
        for _ in range(5):
            yield b"x" * 16384

    content = chunks() if chunked else b"x" * 65537
    response = await client.post(
        "/api/v1/payments", content=content, headers={"Idempotency-Key": str(uuid.uuid4())}
    )
    assert response.status_code == 413
    assert response.json() == {"detail": "Request body too large"}
    # A refused body does not leak an in-flight slot.
    assert (await client.get("/health")).status_code == 200


async def test_auth_still_precedes_limits_and_internal_endpoint(client):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        assert (await http.get("/internal/backlog")).status_code == 401
        assert (await http.post("/api/v1/payments", content=b"x" * 65537)).status_code == 401
    assert (await client.get("/internal/backlog")).status_code == 200


async def test_inflight_rejection_and_release_after_app_error():
    started, release = asyncio.Event(), asyncio.Event()

    async def downstream(scope, receive, send):
        started.set()
        await release.wait()
        raise RuntimeError("downstream failure")

    limiter = RequestLimits(downstream, max_inflight=1, max_body_bytes=64)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    responses = []

    async def send(message):
        responses.append(message)

    scope = {"type": "http", "method": "POST", "path": "/", "headers": []}
    first = asyncio.create_task(limiter(scope, receive, send))
    await started.wait()
    assert limiter.inflight == 1
    await limiter(scope, receive, send)
    assert responses[0]["status"] == 429
    assert limiter.inflight == 1
    release.set()
    with pytest.raises(RuntimeError):
        await first
    assert limiter.inflight == 0
    with pytest.raises(RuntimeError):
        await limiter(scope, receive, send)
    assert limiter.inflight == 0


async def test_streaming_limit_stops_reading_at_first_overflow():
    async def downstream(*args):
        pytest.fail("Oversized body must not reach downstream")

    limiter = RequestLimits(downstream, max_inflight=1, max_body_bytes=64)
    reads = 0
    responses = []

    async def receive():
        nonlocal reads
        reads += 1
        assert reads <= 3
        return {"type": "http.request", "body": b"x" * 32, "more_body": True}

    async def send(message):
        responses.append(message)

    await limiter({"type": "http"}, receive, send)
    assert reads == 3
    assert responses[0]["status"] == 413
    assert limiter.inflight == 0


async def test_slow_body_times_out_and_releases_slot():
    import asyncio
    from unittest.mock import AsyncMock

    from app.core.limits import RequestLimits

    app = AsyncMock()
    wrapper = RequestLimits(app, max_inflight=1, max_body_bytes=100, body_timeout=0.01)
    sent = []

    async def receive():
        await asyncio.sleep(1)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await wrapper({"type": "http"}, receive, send)
    assert sent[0]["status"] == 408
    assert wrapper.inflight == 0
    app.assert_not_awaited()
