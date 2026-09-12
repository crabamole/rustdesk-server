# Copyright 2024 SCTG Development - Ronan LE MEILLAT
# SPDX-License-Identifier: AGPL-3.0-or-later
ARG BASE_IMAGE=sctg/sctgdesk-server-integration:latest
FROM ${BASE_IMAGE} AS builder

FROM ubuntu:jammy
RUN mkdir -p /usr/local/bin
COPY --from=builder /usr/local/bin/hbbs /usr/local/bin/hbbs
COPY --from=builder /usr/local/bin/hbbr /usr/local/bin/hbbr
COPY --from=builder /usr/local/bin/rustdesk-utils /usr/local/bin/rustdesk-utils
RUN mkdir -p /usr/local/share/sctgdesk
WORKDIR /usr/local/share/sctgdesk