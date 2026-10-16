# Build from the repository root:
# docker build -f services/collectors/cli-collector/Dockerfile -t cli-collector .
ARG RUST_IMAGE=rust:1.99-bookworm
FROM ${RUST_IMAGE} AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY contracts/proto ./contracts/proto
COPY libs ./libs
COPY services/collectors ./services/collectors
RUN cargo build --release --locked -p cli-collector \
    && strip target/release/cli-collector

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/cli-collector /usr/local/bin/cli-collector
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/cli-collector"]
