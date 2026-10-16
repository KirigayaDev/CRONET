FROM ghcr.io/amacneil/dbmate:2.36.0 AS dbmate
FROM rust:1-bookworm
RUN rustup component add rustfmt clippy
COPY --from=dbmate /usr/local/bin/dbmate /usr/local/bin/dbmate
WORKDIR /storage
COPY migrations/ migrations/
COPY tests-rs/ tests-rs/
RUN cargo fmt --manifest-path tests-rs/Cargo.toml --all --check \
    && cargo clippy --manifest-path tests-rs/Cargo.toml --locked --all-targets -- -D warnings \
    && cargo test --manifest-path tests-rs/Cargo.toml --locked --no-run
CMD ["cargo", "test", "--manifest-path", "tests-rs/Cargo.toml", "--locked", "--", "--nocapture"]
