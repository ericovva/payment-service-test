import asyncio
import logging
from contextlib import suppress

import httpx
from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.config import get_settings
from app.db import engine, sessions
from app.outbox import relay
from app.processing import Processor, SimulatedGateway, Webhooks
from app.schemas import PaymentEvent

settings = get_settings()
broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
)
queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "payments.dlq"},
)
dlq = RabbitQueue("payments.dlq", durable=True)
client = httpx.AsyncClient(
    timeout=settings.webhook_timeout, follow_redirects=False, trust_env=False
)
processor = Processor(sessions, SimulatedGateway(), Webhooks(client), settings)
app = FastStream(broker)
relay_task: asyncio.Task | None = None


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
    global relay_task
    relay_task = asyncio.create_task(relay(sessions, broker, settings.outbox_poll_seconds))


@app.on_shutdown
async def stop_relay():
    if relay_task:
        relay_task.cancel()
        with suppress(asyncio.CancelledError):
            await relay_task
    await client.aclose()
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(app.run())
