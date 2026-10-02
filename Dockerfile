# syntax=docker/dockerfile:1
FROM node:lts-bookworm AS builder
ENV NODE_VERSION=20.18.0
RUN apt-get update && apt-get install -y curl build-essential pkg-config libssl-dev zip git musl-dev musl-tools perl
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo $(dpkg --print-architecture)
RUN mkdir /build
RUN if [ "$(dpkg --print-architecture)" = "armhf" ]; then \
       . /root/.cargo/env && rustup target add armv7-unknown-linux-musleabihf; \
       ln -svf /usr/bin/ar /usr/bin/arm-linux-musleabihf-ar; \
       echo "armv7-unknown-linux-musleabihf" > /build/_target ; \
    fi
RUN if [ "$(dpkg --print-architecture)" = "arm64" ]; then \
       . /root/.cargo/env && rustup target add aarch64-unknown-linux-musl; \
       ln -svf /usr/bin/ar /usr/bin/aarch64-linux-musl-ar; \
       echo "aarch64-unknown-linux-musl" > /build/_target ; \
    fi
RUN if [ "$(dpkg --print-architecture)" = "amd64" ]; then \
       . /root/.cargo/env && rustup target add x86_64-unknown-linux-musl; \
       echo "x86_64-unknown-linux-musl" > /build/_target ; \
    fi
COPY src /build/src
COPY libs /build/libs
COPY Cargo.toml /build/Cargo.toml
COPY Cargo.lock /build/Cargo.lock
COPY build.rs /build/build.rs
ARG COVERAGE=false
# CI keeps 2 jobs; local builds can pass more.
ARG CARGO_BUILD_JOBS=2
# Cache mounts keep crate downloads and compiled dependencies between builds of this builder.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/build/target \
    export TARGET=$(cat /build/_target) \
    && printf '[net]\ngit-fetch-with-cli = true\n' > /root/.cargo/config.toml \
    && . /root/.cargo/env && cd /build \
    && if [ "$COVERAGE" = "true" ]; then \
        export RUSTFLAGS="-C instrument-coverage --remap-path-prefix=/build=rustdesk-server"; \
        FEATURES="coverage,vendored-openssl"; \
    else \
        FEATURES="vendored-openssl"; \
    fi \
    && export CARGO_TARGET_DIR=/build/target/coverage-$COVERAGE \
    && cargo build --features $FEATURES --target=$TARGET --release -j $CARGO_BUILD_JOBS \
    && mkdir -p /build/output \
    && cp $CARGO_TARGET_DIR/$TARGET/release/hbbr $CARGO_TARGET_DIR/$TARGET/release/hbbs \
        $CARGO_TARGET_DIR/$TARGET/release/rustdesk-utils /build/output/

FROM ubuntu:jammy
ARG COVERAGE=false
COPY --from=builder /build/output/hbbs /usr/local/bin/hbbs
COPY --from=builder /build/output/hbbr /usr/local/bin/hbbr
COPY --from=builder /build/output/rustdesk-utils /usr/local/bin/rustdesk-utils
RUN if [ "$COVERAGE" = "true" ]; then mkdir -p /data/coverage; fi
ENV LLVM_PROFILE_FILE=/data/coverage/%p-%m.profraw
WORKDIR /usr/local/share/rustdesk-server
