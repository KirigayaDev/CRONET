FROM ghcr.io/amacneil/dbmate:2.36.0 AS dbmate
FROM rust:1-bookworm
RUN rustup component add rustfmt clippy
COPY --from=dbmate /usr/local/bin/dbmate /usr/local/bin/dbmate
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/target
CMD ["cargo", "test", "-p", "metrics-batcher", "--locked", "--", "--nocapture"]
