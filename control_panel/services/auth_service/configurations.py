from typing import Any

from pydantic import Field, BaseModel, model_validator
from pydantic_settings import BaseSettings


class _DatabaseSettings(BaseSettings):
    user: str = Field(alias="POSTGRES_USER")
    password: str = Field(alias="POSTGRES_PASSWORD")
    database: str = Field(alias="POSTGRES_DB")


class _JwtSettings(BaseModel):
    public_key: str
    private_key: str

    @model_validator(mode='before')
    @classmethod
    def load_keys(cls, data: Any) -> Any:
        with open("/auth_service/crypt_keys/jwt_keys/private_key.pem", 'r') as f:
            data['private_key'] = f.read()
        with open("/auth_service/crypt_keys/jwt_keys/public_key.pem", 'r') as f:
            data['public_key'] = f.read()
        return data


class _RedisSettings(BaseSettings):
    password: str = Field(alias="REDIS_PASSWORD")


class _DevelopmentModeSettings(BaseSettings):
    development_mode: bool = Field(alias="DEVELOPMENT_MODE", default=False)


jwt_settings = _JwtSettings()
database_settings = _DatabaseSettings()
redis_settings = _RedisSettings()
development_mode_settings = _DevelopmentModeSettings()
