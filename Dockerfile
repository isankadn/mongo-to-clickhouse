# Build Stage
FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    librocksdb-dev \
    libclang-dev \
    clang \
    tzdata \
    && ln -sf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime \
    && echo "Asia/Tokyo" > /etc/timezone \
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

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    librocksdb-dev \
    tzdata \
    && ln -sf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime \
    && echo "Asia/Tokyo" > /etc/timezone \
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