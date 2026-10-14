import jwt

from configurations import jwt_settings
from pydantic import ValidationError

from .schemas import TokenPayloadSchema

_ALGORITHM = "RS256"


def encode_token(payload: TokenPayloadSchema) -> str:
    return jwt.encode(payload.model_dump(mode="json"), jwt_settings.private_key, algorithm=_ALGORITHM)


def decode_token(token: str, verify_nbf=True) -> TokenPayloadSchema | None:
    try:
        payload_dict = jwt.decode(token, jwt_settings.public_key, algorithms=[_ALGORITHM],
                                  audience="Re:simple_videohosting_auth", options={"verify_nbf": verify_nbf})
        decoded = TokenPayloadSchema(**payload_dict)
        return decoded

    except (jwt.PyJWTError, ValidationError):
        return None
