import httpx

from app.payments.models.payment import Payment


class Webhooks:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def send(self, payment: Payment) -> None:
        event_id = f"payment.{payment.id}.completed"
        response = await self.client.post(
            payment.webhook_url,
            headers={"Idempotency-Key": event_id, "X-Event-ID": event_id},
            json={
                "event_id": event_id,
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "metadata": payment.metadata_,
                "processed_at": payment.processed_at.isoformat(),
            },
        )
        response.raise_for_status()
