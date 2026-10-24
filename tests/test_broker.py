import asyncio
import json
import os
import uuid
from unittest.mock import AsyncMock

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from faststream.rabbit import TestRabbitBroker as BrokerHarness
from faststream.rabbit.message import RabbitMessage

from app import worker
from app.schemas import PaymentEvent


async def test_consumer_decodes_and_dispatches_payment(monkeypatch):
    process = AsyncMock()
    monkeypatch.setattr(worker.processor, "process", process)
    payment_id = uuid.uuid4()
    async with BrokerHarness(worker.broker) as broker:
        await broker.publish({"payment_id": str(payment_id), "attempt": 2}, worker.queue)
    process.assert_awaited_once_with(PaymentEvent(payment_id=payment_id, attempt=2))


@pytest.mark.parametrize(
    "body",
    [
        [],
        "garbage",
        b"{broken-json",
        {},
        {"payment_id": "invalid"},
        {"payment_id": str(uuid.uuid4()), "attempt": 4},
    ],
)
async def test_consumer_rejects_malformed_messages(monkeypatch, body):
    process = AsyncMock()
    monkeypatch.setattr(worker.processor, "process", process)
    reject = AsyncMock()
    monkeypatch.setattr(RabbitMessage, "reject", reject)
    async with BrokerHarness(worker.broker) as broker:
        await broker.publish(body, worker.queue)
    reject.assert_awaited_once_with()
    process.assert_not_awaited()


async def test_consumer_rejects_unknown_payment(monkeypatch):
    process = AsyncMock(side_effect=ValueError("Unknown payment"))
    monkeypatch.setattr(worker.processor, "process", process)
    reject = AsyncMock()
    monkeypatch.setattr(RabbitMessage, "reject", reject)
    async with BrokerHarness(worker.broker) as broker:
        await broker.publish({"payment_id": str(uuid.uuid4())}, worker.queue)
    reject.assert_awaited_once_with()
    process.assert_awaited_once()


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("TEST_RABBITMQ_URL"), reason="TEST_RABBITMQ_URL not configured"
)
async def test_real_rabbitmq_persistence_confirms_mandatory_and_dlq():
    """Declare and clean only this test's queues, never the application's queues."""
    prefix = f"test.payments.{uuid.uuid4().hex}"
    queue = RabbitQueue(
        prefix,
        durable=True,
        arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": f"{prefix}.dlq"},
    )
    broker = RabbitBroker(
        os.environ["TEST_RABBITMQ_URL"],
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    source = dead = None
    async with broker:
        try:
            dead = await broker.declare_queue(RabbitQueue(f"{prefix}.dlq", durable=True))
            source = await broker.declare_queue(queue)
            payload = {"payment_id": str(uuid.uuid4()), "attempt": 1}
            await broker.publish(payload, queue=queue, persist=True, mandatory=True, timeout=5)
            message = await source.get(timeout=5)
            assert message is not None
            assert json.loads(message.body) == payload
            assert message.delivery_mode == DeliveryMode.PERSISTENT
            await message.reject(requeue=False)
            # Dead-letter routing is asynchronous. Wait only up to the bounded test deadline.
            async with asyncio.timeout(5):
                while True:
                    rejected = await dead.get(fail=False)
                    if rejected is not None:
                        break
                    await asyncio.sleep(0.05)
            assert json.loads(rejected.body) == payload
            assert rejected.headers["x-death"][0]["reason"] == "rejected"
            await rejected.ack()
            with pytest.raises(DeliveryError):
                await broker.publish(
                    payload, queue=f"{prefix}.missing", persist=True, mandatory=True, timeout=5
                )
        finally:
            if source is not None:
                await source.delete(if_unused=False, if_empty=False)
            if dead is not None:
                await dead.delete(if_unused=False, if_empty=False)
