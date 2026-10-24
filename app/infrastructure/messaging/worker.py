import asyncio
import logging
from contextlib import suppress

import httpx
from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.infrastructure.database.session import engine, sessions
from app.infrastructure.messaging.outbox import relay
from app.payments.integrations.gateway import SimulatedGateway
from app.payments.integrations.webhooks import Webhooks
from app.payments.schemas.payments import PaymentEvent
from app.payments.services.processing import Processor

settings = get_settings()
broker = RabbitBroker(
    settings.rabbitmq_url,
    graceful_timeout=settings.gateway_timeout + settings.webhook_timeout + 5,
    default_channel=Channel(
        prefetch_count=settings.worker_concurrency, publisher_confirms=True, on_return_raises=True
    ),
)
queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "payments.dlq"},
)
dlq = RabbitQueue("payments.dlq", durable=True)
client = httpx.AsyncClient(
    timeout=settings.webhook_timeout,
    follow_redirects=False,
    trust_env=False,
    limits=httpx.Limits(
        max_connections=settings.worker_concurrency,
        max_keepalive_connections=settings.worker_concurrency,
    ),
)
processor = Processor(sessions, SimulatedGateway(), Webhooks(client), settings)
app = FastStream(broker)
relay_task: asyncio.Task | None = None
recovery_task: asyncio.Task | None = None


async def raw_body(message):
    return message.body


@broker.subscriber(queue, retry=True, decoder=raw_body)
async def consume(body: bytes):
    try:
        event = PaymentEvent.model_validate_json(body)
        await processor.process(event)
    except (ValidationError, ValueError) as error:
        raise RejectMessage() from error
    except SQLAlchemyError:
        # Do not consume business retries during a DB outage; let RabbitMQ redeliver.
        await asyncio.sleep(settings.retry_base_seconds)
        raise


@app.on_startup
async def declare_topology():
    await broker.connect()
    await broker.declare_queue(dlq)


@app.after_startup
async def start_relay():
    global relay_task, recovery_task
    relay_task = asyncio.create_task(
        relay(
            sessions,
            broker,
            settings.outbox_poll_seconds,
            settings.outbox_batch_size,
            settings.outbox_publish_concurrency,
            settings.outbox_lease_seconds,
        )
    )
    recovery_task = asyncio.create_task(processor.recovery_loop())


@app.on_shutdown
async def stop_relay():
    for task in (relay_task, recovery_task):
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


@app.after_shutdown
async def close_resources():
    await client.aclose()
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(app.run())
