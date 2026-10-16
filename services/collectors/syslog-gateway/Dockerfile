FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY libs ./libs
COPY contracts/proto ./contracts/proto
COPY services/collectors ./services/collectors
RUN cargo build --release --locked -p syslog-gateway

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --no-create-home syslog \
    && install -d -m 0700 -o 10001 -g 10001 /var/lib/cronet/syslog
COPY --from=builder /build/target/release/syslog-gateway /usr/local/bin/syslog-gateway
ENV SYSLOG_SPOOL_PATH=/var/lib/cronet/syslog/spool.redb
USER 10001:10001
EXPOSE 5514/udp 5514/tcp
VOLUME ["/var/lib/cronet/syslog"]
ENTRYPOINT ["/usr/local/bin/syslog-gateway"]
