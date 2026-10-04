from fastapi import HTTPException
from fastapi.responses import ORJSONResponse

from jwt_tokens.get_data import get_token_data

from database.models.user import User
from database.crud.user.get import get_user_by_uuid

from .router import router


@router.get("/token")
async def authenticate_user(access_token: str) -> ORJSONResponse:
    payload = await get_token_data(access_token)
    if payload is None or payload.token_type != "access":
        raise HTTPException(status_code=401, detail={"msg": "Unauthorized token"})

    user: User = await get_user_by_uuid(payload.sub)

    return ORJSONResponse(status_code=200, content={"msg": "Token info", "user_info": {
        "uuid": str(user.uuid),
        "username": user.username,
        "email": user.email,
        "created_at": user.created_at,
        "token_version_uuid": str(user.token_version_uuid),
        "display_name": user.display_name
    }})
