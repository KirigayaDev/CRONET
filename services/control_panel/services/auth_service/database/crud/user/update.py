from sqlalchemy.exc import SQLAlchemyError

from database.session import async_session
from database.models.user import User


async def try_update_user(user: User) -> bool:
    async with async_session() as session:
        try:
            merged_user = await session.merge(user)  # "привязываем" юзера к сессии
            await session.commit()
            return merged_user.uuid is not None

        except SQLAlchemyError:
            await session.rollback()
            return False
