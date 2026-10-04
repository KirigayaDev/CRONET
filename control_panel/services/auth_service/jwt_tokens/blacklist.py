import datetime

from redis_client import redis_client

from jwt_tokens.schemas import TokenPayloadSchema


def generate_blacklist_token_id(token_id: str) -> None:
    return f'blacklisted_jwt_token:{token_id}'


async def add_token_payload_to_blacklist(payload: TokenPayloadSchema) -> None:
    if payload is not None:
        await redis_client.set(generate_blacklist_token_id(payload.jti), value=1,
                               exat=payload.exp + datetime.timedelta(seconds=10))


async def token_is_blacklisted(payload: TokenPayloadSchema) -> bool:
    return await redis_client.exists(generate_blacklist_token_id(payload.jti))
