import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as DatabasePoolTimeout
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.admission import BacklogGate
from app.core.auth import authenticate
from app.core.config import get_settings
from app.core.limits import RequestLimits
from app.infrastructure.database.session import engine, get_session, sessions
from app.payments.api.routes import router

settings = get_settings()
backlog = BacklogGate(sessions, settings.api_max_pending_payments, settings.backlog_refresh_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await backlog.refresh()
    task = asyncio.create_task(backlog.run())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await engine.dispose()


app = FastAPI(
    title="Payment service", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan
)
app.state.backlog = backlog
app.add_middleware(
    RequestLimits,
    max_inflight=settings.api_max_inflight,
    max_body_bytes=settings.api_max_body_bytes,
    body_timeout=settings.api_body_timeout,
)
app.middleware("http")(authenticate)
app.include_router(router)


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    return app.openapi()


@app.get("/health")
async def health(session: Annotated[AsyncSession, Depends(get_session)]):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/internal/backlog")
async def backlog_snapshot():
    return backlog.snapshot()


@app.exception_handler(DatabasePoolTimeout)
@app.exception_handler(OperationalError)
async def database_unavailable(request, error):
    logging.getLogger(__name__).warning(
        "Database temporarily unavailable: %s", type(error).__name__
    )
    return JSONResponse(
        {"detail": "Database temporarily unavailable"},
        status_code=503,
        headers={"Retry-After": "1"},
    )
