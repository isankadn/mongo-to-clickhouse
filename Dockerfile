# Build Stage
FROM rust:latest as builder

RUN apt-get update && apt-get install -y \
    librocksdb-dev \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*
    
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY historical_data ./historical_data
COPY config-prod.yml ./config-prod.yml
COPY config-dev.yml ./config-dev.yml
ENV RUSTFLAGS="-C target-cpu=native"

RUN cargo build --release

# Production Stage
FROM rust:latest

RUN apt-get update && apt-get install -y \
    librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/mongo-to-clickhouse .
COPY --from=builder /app/target/release/historical_data .

COPY config-prod.yml ./config-prod.yml
COPY config-dev.yml ./config-dev.yml
RUN mkdir -p /data/rocksdb
ENV RUSTFLAGS="-C target-cpu=native"
ENV RUST_ENV=prod 
CMD ["./mongo-to-clickhouse"]