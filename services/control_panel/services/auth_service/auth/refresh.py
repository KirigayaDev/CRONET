import datetime

from fastapi import Cookie, Header

from fastapi.responses import ORJSONResponse

from jwt_tokens.get_data import get_token_data
from jwt_tokens.blacklist import add_token_payload_to_blacklist
from jwt_tokens.generator import generate_auth_tokens
from jwt_tokens.coder import decode_token

from database.crud.user.get import get_user_by_uuid

from .router import router


# get user здесь не используется т к в этот эндпоинт идут запросы когда access token не истекает и нужно выпустить новый
@router.post("/refresh")
async def refresh_tokens(csrf_token: str = Header(None, alias="X-CSRF-Token", alias_priority=True),
                         refresh_token: str = Cookie(None)) -> ORJSONResponse:
    old_refresh_payload = await get_token_data(refresh_token)

    neg_response = None

    if old_refresh_payload is None or old_refresh_payload.token_type != "refresh":
        neg_response = ORJSONResponse(status_code=401, content={"msg": "Unauthorized"})

    csrf_token_payload = decode_token(csrf_token)
    if csrf_token_payload is None or old_refresh_payload.sub != csrf_token_payload.sub or \
            old_refresh_payload.token_version != csrf_token_payload.token_version or \
            csrf_token_payload.token_type != "csrf":
        neg_response = ORJSONResponse(status_code=403, content={"msg": "CSRF validation error"})

    if neg_response is not None:
        neg_response.delete_cookie("access_token")
        neg_response.delete_cookie("refresh_token")
        return neg_response

    user = await get_user_by_uuid(old_refresh_payload.sub)
    new_tokens = generate_auth_tokens(user, datetime.datetime.now())
    await add_token_payload_to_blacklist(old_refresh_payload)

    response = ORJSONResponse(status_code=200, content={"msg": "Successfully refreshed tokens"})
    response.set_cookie("access_token", new_tokens.access_token, httponly=True, secure=True, samesite="strict")
    response.set_cookie("refresh_token", new_tokens.refresh_token, httponly=True, secure=True, samesite="strict")
    return response
