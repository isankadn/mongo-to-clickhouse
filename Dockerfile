# Build Stage
FROM rust:latest as builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY historical_data ./historical_data
COPY config-prod.yml ./config-prod.yml
COPY config-dev.yml ./config-dev.yml
ENV RUSTFLAGS="-C target-cpu=native"

RUN cargo build --release

# Production Stage
FROM rust:slim

WORKDIR /app

COPY --from=builder /app/target/release/mongo-to-clickhouse .
COPY --from=builder /app/target/release/historical_data .

COPY config-prod.yml ./config-prod.yml
COPY config-dev.yml ./config-dev.yml

ENV RUSTFLAGS="-C target-cpu=native"
ENV RUST_ENV=prod 
CMD ["./mongo-to-clickhouse"]