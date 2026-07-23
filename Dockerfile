FROM rust:1.90-bookworm AS builder

WORKDIR /build
COPY . .
RUN cargo build --release -p shard-stream-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home shard-stream \
    && mkdir -p /var/lib/shard-stream \
    && chown shard-stream:shard-stream /var/lib/shard-stream
COPY --from=builder /build/target/release/shard-stream-server /usr/local/bin/shard-stream-server
USER shard-stream
EXPOSE 7420
VOLUME ["/var/lib/shard-stream"]
ENTRYPOINT ["shard-stream-server"]
CMD ["--listen", "0.0.0.0:7420", "--data-dir", "/var/lib/shard-stream"]
