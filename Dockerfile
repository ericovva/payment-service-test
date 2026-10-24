FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/service/.venv/bin:$PATH"
WORKDIR /service

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev \
    && groupadd --system payments \
    && useradd --system --gid payments --home-dir /service payments
COPY app ./app
COPY alembic.ini ./
COPY migrations ./migrations
USER payments
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
