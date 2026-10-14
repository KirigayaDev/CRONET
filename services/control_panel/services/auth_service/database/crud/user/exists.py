from sqlalchemy import exists, and_, select

from pydantic import UUID4

from database.session import async_session
from database.models.user import User


async def user_and_token_version_exists(uuid: UUID4, token_version_uuid: UUID4) -> bool:
    async with async_session() as session:
        query = select(
            exists().where(
                and_(User.uuid == uuid, User.token_version_uuid == token_version_uuid)
            )
        )
        return await session.scalar(query)
