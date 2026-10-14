from pydantic import BaseModel, constr, EmailStr

from .constants import USERNAME_REGEX


class UserLogin(BaseModel):
    login:  EmailStr | constr(pattern=USERNAME_REGEX)
    password: str
