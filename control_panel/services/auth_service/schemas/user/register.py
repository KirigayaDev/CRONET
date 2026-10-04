from pydantic import BaseModel, constr, EmailStr, model_validator
from pydantic_core import PydanticCustomError

from .constants import USERNAME_REGEX


class UserRegisterSchema(BaseModel):
    username: constr(pattern=USERNAME_REGEX)
    email: EmailStr
    password: str
    password2: str
    display_name: constr(min_length=3, max_length=32)

    @model_validator(mode="after")
    def passwords_match(cls, values):
        pw = values.password
        pw2 = values.password2
        if pw != pw2:
            raise PydanticCustomError("passwords_mismatch_error", "passwords do not match")
        return values
