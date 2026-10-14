import ssl

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from configurations import _DatabaseSettings

_DATABASE_SETTINGS = _DatabaseSettings() # type: ignore

DATABASE_URL: str = f"postgresql+asyncpg://{_DATABASE_SETTINGS.user}:{_DATABASE_SETTINGS.password}@postgres:5432/" \
                    f"{_DATABASE_SETTINGS.database}"

engine = create_async_engine(DATABASE_URL, echo=False)

async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)
