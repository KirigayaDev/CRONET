from fastapi import Request, Cookie, Header
from fastapi.exceptions import HTTPException

from database.models.user import User

from jwt_tokens.get_data import get_token_data
from jwt_tokens.coder import decode_token
from jwt_tokens.schemas import TokenPayloadSchema

from database.crud.user.get import get_user_by_uuid


def _validate_csrf_token(request: Request, access_token_payload: TokenPayloadSchema, csrf_token: str) -> bool:
    if request.method in ["GET", "HEAD", "OPTIONS"]:
        return True

    if csrf_token is None:
        return False

    csrf_token_payload = decode_token(csrf_token)
    if csrf_token_payload is None or csrf_token_payload.sub != access_token_payload.sub or \
            csrf_token_payload.token_version != access_token_payload.token_version or \
            csrf_token_payload.token_type != "csrf":
        return False

    return True


async def get_current_user(request: Request, access_token: str = Cookie(None),
                           csrf_token: str = Header(None, alias="X-CSRF-Token", alias_priority=True)) -> User:
    if access_token is None:
        return None

    access_token_payload: TokenPayloadSchema = await get_token_data(access_token)
    if access_token_payload is None:
        raise HTTPException(status_code=401, detail={"msg": "expired access token"})

    if access_token_payload.token_type != "access":
        raise HTTPException(status_code=401, detail={"msg": "Wrong access token"})

    if not _validate_csrf_token(request, access_token_payload, csrf_token):
        raise HTTPException(status_code=403, detail={"msg": "CSRF validation error"})

    user: User = await get_user_by_uuid(access_token_payload.sub)

    return user
