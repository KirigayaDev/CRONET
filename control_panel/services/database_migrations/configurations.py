from pydantic import Field
from pydantic_settings import BaseSettings


class _DatabaseSettings(BaseSettings):
    user: str = Field(..., alias="POSTGRES_USER", alia)
    password: str = Field(..., alias="POSTGRES_PASSWORD")
    database: str = Field(..., alias="POSTGRES_DB")


database_settings = _DatabaseSettings()
