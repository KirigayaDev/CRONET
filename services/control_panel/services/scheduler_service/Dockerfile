# Inspectable, filtered build context; runtime receives only the binary.
FROM scratch AS source
COPY . /

FROM rust:1.89.0-bookworm AS build
WORKDIR /build
COPY --from=source / /build/
RUN cargo build --release --locked -p scheduler-service

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10002 scheduler \
    && useradd --system --uid 10002 --gid 10002 --no-create-home --shell /usr/sbin/nologin scheduler
WORKDIR /app
COPY --from=build /build/target/release/scheduler-service /usr/local/bin/scheduler-service
USER 10002:10002
ENTRYPOINT ["/usr/local/bin/scheduler-service"]
