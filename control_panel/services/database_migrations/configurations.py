from pydantic import Field
from pydantic_settings import BaseSettings


class _DatabaseSettings(BaseSettings):
    user: str = Field(..., alias="POSTGRES_USER", alias_priority=True)
    password: str = Field(..., alias="POSTGRES_PASSWORD", alias_priority=True)
    database: str = Field(..., alias="POSTGRES_DB", alias_priority=True)


database_settings = _DatabaseSettings()
