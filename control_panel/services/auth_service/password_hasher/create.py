from .password_hasher import ph


def create_password_hash(password: str) -> str:
    return ph.hash(password)
