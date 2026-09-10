# sctgdesk-server Fork: Build & Deployment Changes

## Background

Switching from OSS `rustdesk/rustdesk-server` (forked as `rophy/rustdesk-server`) to
`sctg-development/sctgdesk-server` (forked as `rophy/sctgdesk-server`).

sctgdesk-server is an integrated build: hbbs embeds the sctgdesk-api-server crate
directly, providing management API, OIDC, and access control enforcement in one binary.
Key feature: `--logged-in-only` / `LOGGED_IN_ONLY=Y` blocks unauthenticated peer control
at the signaling level.

## What We Get From sctgdesk-server

- TCP/WebSocket mode (Pro feature) — replaces our WS registration patch
- Integrated API server (users, groups, OIDC, address books, audit)
- `LOGGED_IN_ONLY` enforcement in hbbs punch_hole handler
- Web console on port 21114

## Changes To Make

### Phase 1: Build Workflows (priority)

Port the CI/CD pattern from `rophy/rustdesk-server`:

1. **`ci.yaml`** — PR and push-to-default-branch CI
   - `cargo test`
   - Runs on `ubuntu-latest`
   - Source: existing `rophy/rustdesk-server` ci.yaml

2. **`build-amd64.yaml`** — Docker image build and publish
   - Trigger: tag push (`v*.*.*`) and PR (build-only, no push)
   - Version check: tag must match `Cargo.toml` version
   - Build: multi-stage Dockerfile (rust:alpine builder → scratch runtime)
   - Push to `ghcr.io/rophy/sctgdesk-server`
   - amd64 only (no multiarch needed for our deployment)

3. **New `Dockerfile`** — Replace upstream's Dockerfile which pulls from a
   pre-built `sctg/sctgdesk-server-integration:latest` image.
   Our Dockerfile must build from source:
   ```dockerfile
   FROM rust:alpine AS builder
   RUN apk add --no-cache musl-dev pkgconf openssl-dev openssl-libs-static
   WORKDIR /build
   COPY . .
   RUN cargo build --release
   FROM scratch
   COPY --from=builder /build/target/release/hbbs /usr/bin/hbbs
   COPY --from=builder /build/target/release/hbbr /usr/bin/hbbr
   COPY --from=builder /build/target/release/rustdesk-utils /usr/bin/rustdesk-utils
   WORKDIR /data
   ENV HOME=/data
   USER 1000:1000
   ```

Remove upstream workflows that we don't need:
- `multiarch-docker-hub.yml` (Docker Hub, multiarch)
- `macos-intel-build.yml` (macOS binaries)
- `windows.yml` (Windows binaries)
- `ubuntu.yml` (Ubuntu binaries)
- `translate-README.yml` (README translation)
- `main.yml` (orchestrator for above)

### Phase 2: Helm Chart Update

Update `rophy/rustdesk-charts` to deploy sctgdesk-server:

- Change hbbs/hbbr image from `ghcr.io/rophy/rustdesk-server` to
  `ghcr.io/rophy/sctgdesk-server`
- Remove standalone api-server deployment (now integrated into hbbs)
- Add `LOGGED_IN_ONLY` env var to hbbs
- Add `API_SERVER` env var if needed (defaults to `http://127.0.0.1:21114`)
- Port 21114 needs to be exposed for the web console/API
- Existing OIDC callback routes need to work through the integrated server

### Phase 3: Deployment

- Build and push initial Docker image
- Update Helm values
- Deploy to kind-gen1
- Test: OIDC login, `LOGGED_IN_ONLY` enforcement, web console, peer connections

## Not In Scope (future)

- Prometheus metrics (port from rophy/rustdesk-server later)
- Control Role implementation (sctgdesk-server has `LOGGED_IN_ONLY` but not
  granular per-user Control Roles yet)
- Strategy/policy push to clients
- Custom patches to hbbs enforcement logic

## Risks

- **Build complexity**: sctgdesk-server has more dependencies (axum, sqlx, rocket,
  reqwest, sodiumoxide, tokio-tungstenite). Alpine musl build may need extra packages.
- **API compatibility**: sctgdesk-api-server is embedded as a git dependency
  (`sctgdesk-api-server = { git = "..." }`). Upstream changes could break builds.
- **Default branch**: upstream default is `tcpserver-master-build`, not `master`.
  Our fork should use the same.
- **Web client OIDC**: The web client's JS talks to `/api/oidc/auth` etc. With
  the integrated server, these endpoints are served by hbbs on port 21114, but
  the web client is served on a different port/host. CORS and proxy routing need
  to handle this correctly (likely already working via our nginx proxy).
