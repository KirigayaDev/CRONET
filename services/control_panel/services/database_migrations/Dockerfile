FROM python:3.12.12-alpine3.23


WORKDIR /database_migrations
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

RUN addgroup -g 1000 database_migrations && \
    adduser -u 1000 -G database_migrations -D -s /bin/sh database_migrations && \
    chown -R database_migrations:database_migrations /database_migrations &&  chmod -R 775 /database_migrations

USER database_migrations


CMD ["alembic", "upgrade", "head"]