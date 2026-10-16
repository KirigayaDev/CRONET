# This stage also allows inspection of the exact filtered build context.
FROM scratch AS source
COPY . /

FROM rust:1.89.0-bookworm AS build
WORKDIR /build
COPY --from=source / /build/
# Shared contracts use protoc-bin-vendored; no system protoc is needed.
RUN cargo build --release --locked -p cmdb-service

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 cmdb \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin cmdb
WORKDIR /app
COPY --from=build /build/target/release/cmdb-service /usr/local/bin/cmdb-service
USER 10001:10001
EXPOSE 8081 50051
ENTRYPOINT ["/usr/local/bin/cmdb-service"]
