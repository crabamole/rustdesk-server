# syntax=docker/dockerfile:experimental
FROM node:lts-bookworm AS builder
ENV NODE_VERSION=20.18.0
RUN apt-get update && apt-get install -y curl build-essential pkg-config libssl-dev zip git sqlite3 musl-dev musl-tools
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
COPY db_v2/create /build/db_v2/create
COPY Cargo.toml /build/Cargo.toml
COPY Cargo.lock /build/Cargo.lock
COPY build.rs /build/build.rs
RUN mv /root/.cargo /tmp && rm -rf /root/.cargo && mkdir -p /root/.cargo
RUN --mount=type=tmpfs,target=/root/.cargo export TARGET=$(cat /build/_target) \
    && mkdir -p /root/.cargo \
    && cp -av /tmp/.cargo/* /root/.cargo/ && ls -lR /root/.cargo \
    && if [ ! -f /root/.cargo/config.toml ]; then \
        echo "" > /root/.cargo/config.toml; \
    fi && \
    awk 'BEGIN{net_section=0;git_fetch_found=0;printed=0}/^\[net\]/{net_section=1;print;next}/^\[/{if(net_section&&!git_fetch_found){print "git-fetch-with-cli = true";printed=1}net_section=0;print;next}net_section&&/^git-fetch-with-cli\s*=/{print "git-fetch-with-cli = true";git_fetch_found=1;next}{print}END{if(!printed&&!git_fetch_found){if(!net_section)print "\n[net]";print "git-fetch-with-cli = true"}}' /root/.cargo/config.toml > /root/.cargo/config.tmp && \
    mv /root/.cargo/config.tmp /root/.cargo/config.toml \
    && . /root/.cargo/env && cd /build \
    && cargo build --target=$TARGET --release \
    && mkdir -p /build/output \
    && cp /build/target/$(cat /build/_target)/release/hbbr /build/output/ \
    && cp /build/target/$(cat /build/_target)/release/hbbs /build/output/ \
    && cp /build/target/$(cat /build/_target)/release/rustdesk-utils /build/output/

FROM ubuntu:jammy
COPY --from=builder /build/output/hbbs /usr/local/bin/hbbs
COPY --from=builder /build/output/hbbr /usr/local/bin/hbbr
COPY --from=builder /build/output/rustdesk-utils /usr/local/bin/rustdesk-utils
WORKDIR /usr/local/share/sctgdesk
