import datetime

from fastapi import HTTPException, Cookie
from fastapi.responses import ORJSONResponse

from schemas.user.login import UserLogin

from database.crud.user.get import get_user_by_username, get_user_by_email, get_user_by_token_payload
from database.models.user import User

from password_hasher.verify import verify_password

from jwt_tokens.generator import generate_auth_tokens
from jwt_tokens.get_data import get_token_data

from .router import router

from ._validators import _is_email_str


@router.post("/login")
async def login_user(login_data: UserLogin, access_token: str = Cookie(None)) -> ORJSONResponse:
    access_token_payload = await get_token_data(access_token)
    if access_token_payload is not None:
        user: User = await get_user_by_token_payload(access_token_payload)
        if user is not None:
            raise HTTPException(status_code=403, detail={"msg": "Not for Authorized users"})

    if _is_email_str(login_data.login):
        user = await get_user_by_email(login_data.login)

    elif isinstance(login_data.login, str):
        user = await get_user_by_username(login_data.login)

    if user is not None and verify_password(password=login_data.password, password_hash=user.password_hash):
        tokens = generate_auth_tokens(user, datetime.datetime.now())
        response = ORJSONResponse(status_code=200, content={"msg": "Successfully authorized"})
        response.set_cookie("access_token", tokens.access_token, httponly=True, secure=True, samesite="strict")
        response.set_cookie("refresh_token", tokens.refresh_token, httponly=True, secure=True, samesite="strict")
        return response

    raise HTTPException(status_code=401, detail={"msg": "Wrong user data"})
