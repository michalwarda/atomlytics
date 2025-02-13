FROM rust:1.79-slim-buster as build

# create a new empty shell project
RUN USER=root cargo new --bin atomlytics
WORKDIR /atomlytics

# copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./src ./src

# build for release
RUN rm ./target/release/deps/atomlytics*
RUN cargo build --release

# our final base
FROM debian:buster-slim

# copy the build artifact from the build stage
COPY --from=build /atomlytics/target/release/atomlytics .
COPY ./GeoLite2-City.mmdb .
COPY ./regexes.yaml .
COPY ./db ./db
COPY ./src/assets ./src/assets

# set the startup command to run your binary
CMD ["./atomlytics"]
