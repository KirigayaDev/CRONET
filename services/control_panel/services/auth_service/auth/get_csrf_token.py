import datetime

from fastapi import Depends, HTTPException
from fastapi.responses import ORJSONResponse

from database.models.user import User
from jwt_tokens.generator import generate_csrf_token

from .router import router
from ._get_current_user import get_current_user


@router.get("/csrf_token")
async def get_csrf_token(user: User | None = Depends(get_current_user)) -> ORJSONResponse:
    if user is None:
        raise HTTPException(status_code=401, detail={"msg": "Unauthorized"})
    csrf_token = generate_csrf_token(user, datetime.datetime.now())

    return ORJSONResponse(status_code=200, content={"msg": "succesfully generate csrf token",
                                                    "csrf_token": csrf_token})
