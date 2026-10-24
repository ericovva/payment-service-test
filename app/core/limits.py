import asyncio

from starlette.responses import JSONResponse


class RequestLimits:
    """Bound per-process concurrent work and buffered request bodies before parsing JSON."""

    def __init__(self, app, max_inflight: int, max_body_bytes: int, body_timeout: float = 10):
        self.app = app
        self.max_inflight = max_inflight
        self.max_body_bytes = max_body_bytes
        self.inflight = 0
        self.body_timeout = body_timeout

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        if self.inflight >= self.max_inflight:
            response = JSONResponse(
                {"detail": "Too many in-flight requests"}, 429, headers={"Retry-After": "1"}
            )
            return await response(scope, receive, send)
        self.inflight += 1
        try:
            body = bytearray()
            try:
                async with asyncio.timeout(self.body_timeout):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        chunk = message.get("body", b"")
                        if len(body) + len(chunk) > self.max_body_bytes:
                            response = JSONResponse({"detail": "Request body too large"}, 413)
                            return await response(scope, receive, send)
                        body.extend(chunk)
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                response = JSONResponse({"detail": "Request body timeout"}, 408)
                return await response(scope, receive, send)
            sent = False

            async def bounded_receive():
                nonlocal sent
                if not sent:
                    sent = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.app(scope, bounded_receive, send)
        finally:
            self.inflight -= 1
