from datetime import datetime

from pydantic import BaseModel, field_serializer
from pydantic.types import UUID4

from typing import Literal


class TokenPayloadSchema(BaseModel):
    iss: str = 'Re:simple_videohosting_auth'
    sub: UUID4
    aud: str = 'Re:simple_videohosting_auth'
    exp: datetime
    nbf: datetime
    iat: datetime
    jti: UUID4
    token_version: UUID4
    token_type: Literal["access", "refresh", "csrf"]

    @field_serializer("exp", "nbf", "iat")
    def serialize_datetime_as_timestamp(self, dt: datetime, _info):
        return dt.timestamp()


class UserJwtTokens(BaseModel):
    access_token: str
    refresh_token: str
