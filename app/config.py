from functools import lru_cache

from pydantic import Field, SecretStr
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
    outbox_poll_seconds: float = Field(default=0.5, gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
