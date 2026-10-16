FROM rust:1-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release -p metrics-batcher \
    && cp target/release/metrics-batcher /metrics-batcher

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /metrics-batcher /usr/local/bin/metrics-batcher
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/metrics-batcher"]
