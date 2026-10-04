from sqlalchemy.exc import SQLAlchemyError

from database.session import async_session
from database.models.user import User

from schemas.user.register import UserRegisterSchema
from password_hasher.create import create_password_hash


async def try_create_user(user_data: UserRegisterSchema) -> bool:
    async with async_session() as session:
        try:
            user: User = User(username=user_data.username, password_hash=create_password_hash(user_data.password),
                              email=user_data.email, display_name=user_data.display_name)
            session.add(user)
            await session.commit()
            return user.uuid is not None

        except SQLAlchemyError:
            await session.rollback()
            return False
