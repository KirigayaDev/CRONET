from pydantic import BaseModel, model_validator
from pydantic_core import PydanticCustomError


class UserChangePasswordSchema(BaseModel):
    old_password: str
    new_password: str
    new_password2: str

    @model_validator(mode="after")
    def passwords_match(cls, values):
        pw = values.new_password
        pw2 = values.new_password2
        if pw != pw2:
            raise PydanticCustomError("passwords_mismatch_error", "passwords do not match")
        return values
