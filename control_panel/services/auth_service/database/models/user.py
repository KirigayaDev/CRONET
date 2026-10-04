from sqlalchemy import Column, String, TIMESTAMP, func, text

from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class User(Base):
    __tablename__ = 'users'

    uuid = Column(UUID(as_uuid=True), primary_key=True, index=True, name='uuid',
                  server_default=text('uuid_generate_v4()'))
    username = Column(String(32), unique=True, index=True, nullable=False, name='username')
    email = Column(String(254), unique=True, index=True, nullable=False, name='email')
    password_hash = Column(String, nullable=False, name='password_hash')
    created_at = Column(TIMESTAMP, server_default=func.nowtime(), name='created_at')
    token_version_uuid = Column(UUID(as_uuid=True), nullable=False, name='token_version_uuid',
                                server_default=text('uuid_generate_v4()'))
    display_name = Column(String(32), nullable=False, name="display_name")
