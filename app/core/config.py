from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://payments:payments@localhost/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost/"
    api_key: SecretStr = Field(min_length=16)
    webhook_allowed_hosts: list[str] = []
    webhook_allow_http: bool = False
    webhook_timeout: float = Field(default=10, gt=0)
    retry_base_seconds: float = Field(default=1, gt=0)
    outbox_poll_seconds: float = Field(default=0.1, gt=0)
    database_pool_size: int = Field(default=10, ge=1, le=200)
    database_max_overflow: int = Field(default=0, ge=0, le=200)
    database_pool_timeout: float = Field(default=2, gt=0)
    worker_concurrency: int = Field(default=1, ge=1, le=1000)
    gateway_timeout: float = Field(default=10, gt=0)
    processing_lease_seconds: float = Field(default=45, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_publish_concurrency: int = Field(default=10, ge=1, le=100)
    outbox_lease_seconds: float = Field(default=30, ge=5)
    api_max_pending_payments: int = Field(default=100000, ge=0)
    backlog_refresh_seconds: float = Field(default=1, gt=0)
    api_max_inflight: int = Field(default=256, ge=1)
    api_body_timeout: float = Field(default=10, gt=0)
    api_max_body_bytes: int = Field(default=65536, ge=1024)

    @model_validator(mode="after")
    def lease_covers_network_timeouts(self):
        if self.processing_lease_seconds <= self.gateway_timeout + self.webhook_timeout + 5:
            raise ValueError("Processing lease must cover gateway + webhook timeouts + 5 seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
