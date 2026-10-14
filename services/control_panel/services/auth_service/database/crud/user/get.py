from sqlalchemy import select

from pydantic import UUID4, constr, EmailStr

from database.session import async_session
from database.models.user import User

from jwt_tokens.schemas import TokenPayloadSchema

from schemas.user.constants import USERNAME_REGEX


async def get_user_by_uuid(uuid: UUID4) -> User:
    async with async_session() as session:
        result = await session.execute(select(User).where(User.uuid == uuid))
        return result.scalar_one_or_none()


async def get_user_by_username(username: constr(pattern=USERNAME_REGEX)) -> User:
    async with async_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()


async def get_user_by_email(email: EmailStr) -> User:
    async with async_session() as session:
        result = await session.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()


async def get_user_by_token_payload(token_payload: TokenPayloadSchema) -> User:
    user = await get_user_by_uuid(token_payload.sub)
    if user.token_version_uuid != token_payload.token_version:
        return None

    return user
