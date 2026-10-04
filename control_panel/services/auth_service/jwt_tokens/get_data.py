from database.crud.user.exists import user_and_token_version_exists

from .blacklist import token_is_blacklisted
from .coder import decode_token
from .schemas import TokenPayloadSchema


async def get_token_data(token: str, verify_nbf=True) -> TokenPayloadSchema | None:
    decoded: TokenPayloadSchema = decode_token(token, verify_nbf=verify_nbf)
    if decoded is None or await token_is_blacklisted(decoded) or \
            not await user_and_token_version_exists(decoded.sub, decoded.token_version):
        return None

    return decoded
