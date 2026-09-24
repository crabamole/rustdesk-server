# rustdesk-server

RustDesk ID/rendezvous server (`hbbs`) and relay server (`hbbr`), built to run
together with [rustdesk-api](https://github.com/crabamole/rustdesk-api)
on a shared PostgreSQL database.

This is a fork of [sctg-development/sctgdesk-server](https://github.com/sctg-development/sctgdesk-server),
itself based on [rustdesk/rustdesk-server](https://github.com/rustdesk/rustdesk-server).
Compared to the upstream open-source server it adds:

- **WebSocket endpoints** for rendezvous (`21118`) and relay (`21119`), including
  peer registration over WebSocket, so desktop clients and the RustDesk web client
  can reach the server through a single HTTPS reverse proxy.
- **PostgreSQL** peer storage, shared with rustdesk-api.
- **Login enforcement**: with `LOGGED_IN_ONLY=Y`, hbbs rejects connection requests
  from clients that are not logged in, validating their tokens against the api-server.

The API server and web console are no longer embedded; they live in
rustdesk-api.

## Components and ports

| Binary | Role | Ports |
| --- | --- | --- |
| `hbbs` | ID/rendezvous server | `21116` TCP+UDP (rendezvous), `21115` TCP (NAT test), `21118` TCP (WebSocket) |
| `hbbr` | Relay server | `21117` TCP (relay), `21119` TCP (WebSocket) |
| `rustdesk-utils` | CLI utilities, e.g. `genkeypair` | — |

The NAT test and WebSocket ports follow the main port: `PORT - 1` and `PORT + 2`.

## Requirements

- **PostgreSQL**, shared with rustdesk-api. The api-server owns the schema:
  start it first against the same database. hbbs never creates or migrates tables;
  at startup it waits until the schema exists.
- **rustdesk-api**, reachable from hbbs when `LOGGED_IN_ONLY=Y` (set its URL
  with `API_SERVER`).

## Deployment

### Kubernetes (recommended)

The Helm chart in [crabamole/rustdesk-charts](https://github.com/crabamole/rustdesk-charts)
deploys hbbs, hbbr, rustdesk-api, the web client and a bundled PostgreSQL.
See its README for installation and values.

### Docker

Images are published to `ghcr.io/crabamole/rustdesk-server:<version>` (and `:latest`).
The binaries are in `/usr/local/bin`; the working directory is
`/usr/local/share/rustdesk-server`, where hbbs keeps its keypair, so mount a volume there.

```bash
docker run -d --name hbbr \
  -p 21117:21117 -p 21119:21119 \
  -v "$PWD/data:/usr/local/share/rustdesk-server" \
  ghcr.io/crabamole/rustdesk-server:latest hbbr

docker run -d --name hbbs \
  -p 21115:21115 -p 21116:21116 -p 21116:21116/udp -p 21118:21118 \
  -v "$PWD/data:/usr/local/share/rustdesk-server" \
  -e DB_URL=postgres://rustdesk:secret@db.example.com:5432/rustdesk \
  -e API_SERVER=http://api.example.com:21114 \
  ghcr.io/crabamole/rustdesk-server:latest hbbs -r relay.example.com:21117
```

## Keypair

On first start hbbs writes `id_ed25519` / `id_ed25519.pub` to its working directory
and logs the public key; clients need that key. To create a keypair yourself:

```bash
docker run --rm --entrypoint /usr/local/bin/rustdesk-utils ghcr.io/crabamole/rustdesk-server:latest genkeypair
```

## Configuration

Options can be given as command-line flags, as environment variables, in a `.env`
file in the working directory, or in an INI file passed with `-c`.

### hbbs

| Flag | Environment | Description |
| --- | --- | --- |
| | `DB_URL` | **Required.** PostgreSQL URL, `postgres://user:pass@host:5432/db` |
| | `MAX_DATABASE_CONNECTIONS` | Connection pool size (default `num_cpus * 4`). hbbs and the api-server each open a pool, so keep the total within Postgres `max_connections` |
| | `API_SERVER` | rustdesk-api URL used to validate tokens (default `http://127.0.0.1:21114`) |
| `--logged-in-only` | `LOGGED_IN_ONLY=Y` | Only logged-in clients may control peers |
| | `ALWAYS_USE_RELAY=Y` | Disallow direct peer connections |
| `-p, --port` | `PORT` | Rendezvous port (default `21116`) |
| `-k, --key` | `KEY` | Only allow clients with this key. The default `-` uses the keypair in the working directory, generated on first start |
| `-r, --relay-servers` | | Relay servers handed to clients, comma-separated |
| `-R, --rendezvous-servers` | | Rendezvous servers, comma-separated |
| `--mask` | | LAN range, e.g. `192.168.0.0/16`, used to detect LAN connections |
| `-M, --rmem` | | UDP receive buffer size (raise the system `net.core.rmem_max` first) |
| `-u, --software-url` | | Download URL of the newest RustDesk client |

### hbbr

| Flag | Environment | Description |
| --- | --- | --- |
| `-p, --port` | `PORT` | Relay port (default `21117`) |
| `-k, --key` | `KEY` | Only allow clients with this key (`-`: use the working-directory keypair) |
| | `LIMIT_SPEED` | Speed limit (Mb/s) |
| | `TOTAL_BANDWIDTH` | Max total bandwidth (Mb/s) |
| | `SINGLE_BANDWIDTH` | Max bandwidth per connection (Mb/s) |
| | `DOWNGRADE_THRESHOLD` | Threshold of the downgrade check (bit/ms) |
| | `DOWNGRADE_START_CHECK` | Delay before the downgrade check (seconds) |

Both binaries honour `RUST_LOG` (`error`, `warn`, `info`, `debug`, `trace`).

## Building

Requires a Rust toolchain.

```bash
cargo build --release     # target/release/{hbbs,hbbr,rustdesk-utils}
docker build -t rustdesk-server .
```

To run hbbs locally against a throwaway PostgreSQL:

```bash
docker run -d --name hbbs-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17-alpine
# start rustdesk-api with DATABASE_URL pointing at the same database first; it creates the schema
DB_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres ./target/release/hbbs
```

## Tests

```bash
make test   # unit + integration tests with coverage; needs Docker for PostgreSQL
```

## License

AGPL-3.0, see [LICENSE](LICENSE).
