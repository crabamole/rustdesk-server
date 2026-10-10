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
| `hbbs` | ID/rendezvous server | `21116` TCP+UDP (rendezvous), `21115` TCP (NAT test), `21118` TCP (WebSocket), `21120` TCP (pod-to-pod, cluster only), `21121` HTTP (`/livez`, `/readyz`) |
| `hbbr` | Relay server | `21117` TCP (relay), `21119` TCP (WebSocket), `21122` TCP (HTTP health) |
| `rustdesk-utils` | CLI utilities, e.g. `genkeypair` | — |

The NAT test and WebSocket ports follow the main port: `PORT - 1` and `PORT + 2`.

## Requirements

- **PostgreSQL**, shared with rustdesk-api. The api-server owns the schema:
  start it first against the same database. hbbs never creates or migrates tables;
  it needs the `peer`, `hbbs_pod` and `peer_presence` tables and at startup waits until they exist.
- **rustdesk-api**, reachable from hbbs (set its URL with `API_SERVER`). Required
  when `LOGGED_IN_ONLY=Y`; also used, best effort, for any request carrying a
  token so the connection can be attributed to the signed-in user.

## Deployment

### Kubernetes (recommended)

The Helm chart in [crabamole/rustdesk-charts](https://github.com/crabamole/rustdesk-charts)
deploys hbbs, hbbr, rustdesk-api, the web client and a bundled PostgreSQL.
See its README for installation and values.

### Docker

Images are published to `ghcr.io/crabamole/rustdesk-server:<version>`.
The binaries are in `/usr/local/bin`; the working directory is
`/usr/local/share/rustdesk-server`, where hbbs keeps its keypair, so mount a volume there.

```bash
docker run -d --name hbbr \
  -p 21117:21117 -p 21119:21119 \
  -v "$PWD/data:/usr/local/share/rustdesk-server" \
  ghcr.io/crabamole/rustdesk-server:1.1.16-2 hbbr

docker run -d --name hbbs \
  -p 21115:21115 -p 21116:21116 -p 21116:21116/udp -p 21118:21118 \
  -v "$PWD/data:/usr/local/share/rustdesk-server" \
  -e DB_URL=postgres://rustdesk:secret@db.example.com:5432/rustdesk \
  -e API_SERVER=http://api.example.com:21114 \
  -e RELAY_URLS=ws://relay.example.com:21119 \
  ghcr.io/crabamole/rustdesk-server:1.1.16-2 hbbs
```

## Keypair

On first start hbbs writes `id_ed25519` / `id_ed25519.pub` to its working directory
and logs the public key; clients need that key. To create a keypair yourself:

```bash
docker run --rm --entrypoint /usr/local/bin/rustdesk-utils ghcr.io/crabamole/rustdesk-server:1.1.16-2 genkeypair
```

Clients built from [crabamole/rustdesk](https://github.com/crabamole/rustdesk) trust only the
`custom.txt` signing key given at build time (`RUSTDESK_CUSTOM_CLIENT_PK`). Use a separate keypair
for it, keep its secret key offline, and sign a JSON config into `custom.txt`:

```bash
rustdesk-utils signcustom custom.json secret-key-file > custom.txt
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
| | `POD_NAME` | Pod name; its trailing number (0–31) is the pod's ordinal (default `hbbs-0`) |
| | `POD_IP` | Address other hbbs pods reach this one on (default `127.0.0.1`) |
| | `INTERNAL_PORT` | Pod-to-pod forwarding port (default `21120`); reachable from hbbs pods only |
| | `HEALTH_PORT` | HTTP port for `/livez` and `/readyz` (default `21121`) |
| | `KEEP_ALIVE_SECS` | Keep-alive sent to devices at registration (default `20`); a device drops a connection silent for 1.5 × this |
| `-p, --port` | `PORT` | Rendezvous port (default `21116`) |
| `-k, --key` | `KEY` | Only allow clients with this key. The default `-` uses the keypair in the working directory, generated on first start. hbbs needs the secret key (pod-to-pod calls authenticate with a token derived from it), so a bare public key is refused |
| | `RELAY_URLS` | **Required.** `ws://` or `wss://` relay URLs handed to clients, comma-separated, e.g. `wss://host/ws/relay/0,wss://host/ws/relay/1`; one is picked at random per session. Clients dial the URL exactly as given and have no relay setting of their own; hbbs forwards only relays from this list and refuses to start without a valid one |
| | `RELAY_CHECKS` | Optional health URLs, same order and length as `RELAY_URLS`, polled every 3 s (e.g. `http://rustdesk-hbbr-0:21122/readyz`); only relays answering 200 are handed out, all of them when none does |
| `-R, --rendezvous-servers` | | Rendezvous servers, comma-separated |
| `--mask` | | LAN range, e.g. `192.168.0.0/16`, used to detect LAN connections |
| `-M, --rmem` | | UDP receive buffer size (raise the system `net.core.rmem_max` first) |
| `-u, --software-url` | | Download URL of the newest RustDesk client |

### hbbr

| Flag | Environment | Description |
| --- | --- | --- |
| `-p, --port` | `PORT` | Relay port (default `21117`) |
| `-k, --key` | `KEY` | Only allow clients with this key (`-`: use the working-directory keypair) |
| | `HEALTH_PORT` | HTTP health port (default `21122`): `GET /livez`, `GET /readyz` |
| | `LIMIT_SPEED` | Speed limit (Mb/s) |
| | `TOTAL_BANDWIDTH` | Max total bandwidth (Mb/s) |
| | `SINGLE_BANDWIDTH` | Max bandwidth per connection (Mb/s) |
| | `DOWNGRADE_THRESHOLD` | Threshold of the downgrade check (bit/ms) |
| | `DOWNGRADE_START_CHECK` | Delay before the downgrade check (seconds) |

On SIGTERM hbbr fails `/readyz` and keeps accepting and relaying; it exits once no session is open or waiting for its pair, at the earliest 10 s after the signal. SIGINT and SIGQUIT exit at once, also while draining. A connection that does not finish the WebSocket upgrade within 30 s is dropped. Give the pod a termination grace period long enough for sessions to end.

Both binaries honour `RUST_LOG` (`error`, `warn`, `info`, `debug`, `trace`).

## Multiple replicas

Several hbbs processes can share one database, e.g. as the pods of a Kubernetes StatefulSet
(`POD_NAME` ends in the pod's ordinal, 0–31; `POD_IP` is its address).

- Each pod records the devices it holds in `peer_presence` and refreshes its `hbbs_pod` row every
  10 s; rows of a pod silent for 30 s, or of an earlier process, are ignored.
- A connection request for a device on another pod is forwarded to it over `INTERNAL_PORT`; the
  device's answer, wherever it arrives, is passed back to the pod holding the viewer. Only hbbs pods
  must reach that port.
- `OnlineRequest` on `/ws/id` is answered from presence across all pods.
- Every viewer's port is replaced by a routing token naming its pod, so clients on raw TCP 21116 always
  use the relay: the port in `PunchHole`/`PunchHoleResponse` and in `TestNatResponse` is that token.
  WebSocket clients behind the proxy are unaffected; their port was never usable for punching.
- The per-IP registration limits (`IP_BLOCK_PER_MINUTE`, `IP_BLOCK_IDS_PER_DAY`) count per pod, so the
  effective limit is the number of pods times the setting.
- On SIGTERM hbbs fails `/readyz`, deletes its presence rows and closes its device connections, so
  devices reconnect to other pods at once, then exits. Give the pod a short `preStop` pause so the
  Service drops it before the connections close.

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
