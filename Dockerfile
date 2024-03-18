FROM rust:latest


WORKDIR /app


COPY Cargo.toml Cargo.lock ./

RUN cargo build --release
RUN rm src/*.rs


COPY src ./src


RUN cargo install cargo-watch


CMD ["cargo", "watch", "-x", "run"]