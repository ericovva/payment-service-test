from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import get_settings

settings = get_settings()
pool_options = {}
if not settings.database_url.startswith("sqlite"):
    pool_options = dict(
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
    )
engine = create_async_engine(settings.database_url, pool_pre_ping=True, **pool_options)
sessions = async_sessionmaker(engine, expire_on_commit=False)


async def get_session():
    async with sessions() as session:
        yield session
