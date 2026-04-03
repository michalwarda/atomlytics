FROM node:24-bookworm AS assets
WORKDIR /workspace

COPY package.json pnpm-lock.yaml ./
COPY app/src ./app/src
RUN corepack enable
RUN pnpm install --frozen-lockfile
RUN pnpm build:css

FROM rust:1.91-bookworm AS dev
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates nodejs npm pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g pnpm
RUN cargo install cargo-watch --locked

FROM rust:1.91-bookworm AS builder
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY app/Cargo.toml ./app/Cargo.toml
COPY migration/Cargo.toml ./migration/Cargo.toml
COPY app/src ./app/src
COPY migration/src ./migration/src
COPY regexes.yaml ./regexes.yaml
COPY GeoLite2-City.mmdb ./GeoLite2-City.mmdb
COPY --from=assets /workspace/app/src/assets/app.css ./app/src/assets/app.css

RUN cargo build --release -p atomlytics

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/atomlytics ./atomlytics
COPY --from=builder /workspace/regexes.yaml ./regexes.yaml
COPY --from=builder /workspace/GeoLite2-City.mmdb ./GeoLite2-City.mmdb
COPY --from=builder /workspace/app/src/assets ./app/src/assets

ENV PORT=3000
EXPOSE 3000

CMD ["./atomlytics"]
