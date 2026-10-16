FROM rust:1-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release -p logs-batcher \
    && cp target/release/logs-batcher /logs-batcher

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /logs-batcher /usr/local/bin/logs-batcher
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/logs-batcher"]
