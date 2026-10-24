import secrets

from fastapi.responses import JSONResponse

from app.core.config import get_settings


async def authenticate(request, call_next):
    provided = request.headers.get("X-API-Key", "")
    expected = get_settings().api_key.get_secret_value()
    if not secrets.compare_digest(provided.encode(), expected.encode()):
        return JSONResponse({"detail": "Invalid API key"}, status_code=401)
    return await call_next(request)
