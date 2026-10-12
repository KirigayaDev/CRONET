import asyncio

from fastapi import Cookie, Depends, HTTPException, Header

from fastapi.responses import ORJSONResponse

from jwt_tokens.get_data import get_token_data
from jwt_tokens.blacklist import add_token_payload_to_blacklist

from database.models.user import User

from .router import router

from ._get_current_user import get_current_user


@router.post("/logout")
async def add_tokens_to_blacklist(access_token: str = Cookie(None), refresh_token: str = Cookie(None),
                                  user: User | None = Depends(get_current_user)) -> ORJSONResponse:
    if user is None:
        raise HTTPException(status_code=401, detail={"msg": "Unauthorized"})

    access_payload = await get_token_data(access_token)
    refresh_payload = await get_token_data(refresh_token, verify_nbf=False)
    await asyncio.gather(add_token_payload_to_blacklist(access_payload),
                         add_token_payload_to_blacklist(refresh_payload))

    response = ORJSONResponse(status_code=200, content={"msg": "Successfully logout"})
    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")
    return response
