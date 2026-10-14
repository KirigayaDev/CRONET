from argon2.exceptions import VerifyMismatchError

from .password_hasher import ph


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return ph.verify(password_hash, password)

    except VerifyMismatchError:
        return False
