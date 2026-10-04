from fastapi import status, Depends, HTTPException
from fastapi.responses import ORJSONResponse

from schemas.user.register import UserRegisterSchema

from database.crud.user.create import try_create_user
from database.models.user import User

from .router import router
from ._get_current_user import get_current_user


@router.post("/register/")
async def try_register_user(user_data: UserRegisterSchema,
                            user: User | None = Depends(get_current_user)) -> ORJSONResponse:
    if user is not None:
        raise HTTPException(403, detail={"msg": "Not fot Authorized users"})
    result = await try_create_user(user_data)
    result_dict = {"message": "Success"} if result else {"message": "Failure"}
    status_code = status.HTTP_201_CREATED if result else status.HTTP_400_BAD_REQUEST
    return ORJSONResponse(status_code=status_code, content=result_dict)
