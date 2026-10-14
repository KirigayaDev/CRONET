from pydantic import ValidationError, TypeAdapter, EmailStr

_email_adapter = TypeAdapter(EmailStr)


def _is_email_str(value: str) -> bool:
    try:
        _email_adapter.validate_python(value)
        return True
    except ValidationError:
        return False
