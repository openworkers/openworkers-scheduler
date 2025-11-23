FROM rust:1.91 AS builder

RUN mkdir -p /build

ENV RUST_BACKTRACE=1
ENV SQLX_OFFLINE=true

WORKDIR /build

COPY . /build

RUN --mount=type=cache,target=$CARGO_HOME/git \
    --mount=type=cache,target=$CARGO_HOME/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release && cp /build/target/release/openworkers-scheduler /build/output

FROM debian:bookworm-slim

RUN apt-get update \
    # Install ca-certificates and wget (used for healthcheck)
    && apt-get install -y ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/output /usr/local/bin/openworkers-scheduler

CMD ["/usr/local/bin/openworkers-scheduler"]
