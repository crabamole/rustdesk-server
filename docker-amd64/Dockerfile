FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconf openssl-dev openssl-libs-static file make gcc g++ sqlite nodejs npm

WORKDIR /build
COPY . .
RUN DATABASE_URL=sqlite:///build/db_v2.sqlite3 CARGO_BUILD_JOBS=2 cargo build --release

FROM scratch
COPY --from=builder /build/target/release/hbbs /usr/bin/hbbs
COPY --from=builder /build/target/release/hbbr /usr/bin/hbbr
COPY --from=builder /build/target/release/rustdesk-utils /usr/bin/rustdesk-utils
WORKDIR /data
ENV HOME=/data
USER 1000:1000
