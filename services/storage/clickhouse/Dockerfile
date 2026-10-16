FROM ghcr.io/amacneil/dbmate:2.36.0
COPY migrations/ /db/migrations/
CMD ["--migrations-dir", "/db/migrations", "--no-dump-schema", "up", "--strict"]
