# sync-tables

Copy PostgreSQL tables from a `dev` warehouse to a developer's local database, or
mirror them to `prod`, `staging` or `preprod`. Copies are incremental (on a date watermark, with an optional
re-copied window) or full, views are recreated from their definition, and the copy
runs **one way only**: `dev → local`, `dev → prod`, `dev → staging` and `dev → preprod`,
never the other ways. staging and preprod are read-only copies, like prod: nothing leaves them.

The tool has two roles. A **source** reads its database and sends what the target is
missing. A **sink** holds the target database, checks every guard itself, and applies
what it receives, one transaction per table. For `dev → local` the local sink pulls
over WSS (TLS and a personal token) from a source serving beside dev's Postgres, so no
tunnel and no dev credentials on the laptop; with dev's database in reach, both
roles can also run in one process. For `dev → prod` the source pushes over WSS (mutual
TLS and a token) to a sink running beside prod's Postgres, which holds prod's credentials. Whichever side
dials, rows flow from the source to the sink only.

The design, the security model and the operating guide are in [`docs/`](docs/README.md).

## Setup

```bash
uv sync
cp .env.example .env              # DB_LOCAL, and for a pull SYNC_SOURCE_URL, SYNC_TLS_CA
                                  # (the pull CA's certificate) and SYNC_TOKEN (your own token)
cp config.sample.yaml config.yaml # optional: global settings and a table list
```

`.env`, `config.yaml`, `config/`, `pki/` and `taskfile.local.yml` are gitignored: real
connection settings, table lists and client material stay outside the repository.

## Usage

Every run names its edge with `--from` and `--to`.

```bash
# dev -> local with the named config `orders`, pulled over WSS (SYNC_* from .env)
uv run sync-tables orders --from dev --to local

# The same with every option spelled out
SYNC_TOKEN=… uv run sync-tables orders --from dev --to local \
  --source-url wss://sync-source.example.org --ca pki/pull-ca.crt

# Full refresh: truncate the target tables and reload them
uv run sync-tables orders --from dev --to local --full-refresh

# A profile file instead of a named config
uv run sync-tables --from dev --to local --profile ./my-profile.yaml

# List the named configs
uv run sync-tables --list
```

Exit codes: `0` everything copied, `1` some tables failed (the others are copied),
`2` the run was refused or could not start.

## Environments and the guards

| Edge | Transport |
|---|---|
| `dev → local` | pull: this process dials a serving source beside dev over WSS and writes the local database. Or direct, without `SYNC_SOURCE_URL`: it reads `DB_DEV` itself |
| `dev → prod` | push: this process (beside dev) sends to a sink in prod over WSS |
| `dev → staging`, `dev → preprod` | push, as `dev → prod`, to that environment's sink |

The transport follows from the URL given: `--source-url` pulls, `--sink-url` pushes,
neither copies directly. An edge refuses a transport it does not use.

Every other edge, including the reverse of each, is refused before any connection is
opened. The edges are compiled into the tool; no configuration can add one.

Each database says which environment it is, as a comment on the database:

```sql
COMMENT ON DATABASE warehouse IS 'celine:env=local';   -- or dev, prod
```

The tool reads it on both sides and refuses a run when a marker is missing,
malformed or does not match the edge. It never writes a marker: the database's owner
sets it once (for dev and prod, best in the deployment that creates the database). The
tool also refuses when source and target are the same
Postgres cluster, whatever the markers say. It reads the source in a read-only
transaction. The sink writes only inside the schemas it allows: for `dev → local`,
the profile's schemas; for a prod sink, its own mounted allowlist.

## Configuration

`config.yaml` holds the global settings (database URLs, parallelism, batch size), and
named configs under `config/` next to it hold table lists. Both are optional: without
`config.yaml` the tool reads `DB_DEV` and `DB_LOCAL` from the environment, with
parallelism 4 and batches of 5000, and a run takes its tables from `--profile FILE`.
The image ships neither. The repository ships only
[`config.sample.yaml`](config.sample.yaml), which loads as it is (a test checks it).

```yaml
# config.yaml
source_db: { url: "${DB_DEV}" }
destination_db: { url: "${DB_LOCAL}" }
parallelism: 4
batch_size: 5000
create_table_if_missing: true
```

Each named config contains only a `sources` list:

```yaml
sources:
  - schema: silver
    table: orders
    date_column: ts
```

A `type: path` entry expands to the dbt models of a pipeline instead
(`<pipelines_root>/<pipeline>/dbt/models/<layer>/<pattern>`, schema
`<schema_prefix>_<layer>`, the date column read from the model's incremental block).
It needs `pipelines_root` in `config.yaml`, relative to it, and `schema_prefix` in the
entry.

### Source entry fields

| Field         | Required | Description                                           |
|---------------|----------|-------------------------------------------------------|
| `schema`      | yes      | Target schema name                                    |
| `table`       | yes      | Table name                                            |
| `date_column` | no       | Column used for incremental sync watermark            |
| `cutoff`      | no       | Only sync rows within this window (e.g. `2d`)         |
| `lookback`    | no       | Re-copy this trailing window on every run (`2d`, `6h`) |

When `date_column` is set, sync is incremental: only rows newer than
`max(date_column)` in the target are fetched. Without it, the table is
truncated and fully reloaded each run.

`cutoff` bounds what is fetched at all: rows older than `now - cutoff` are never
copied. `lookback` is for tables a producer recomputes over a recent window. Each run
deletes the target rows with `date_column >= watermark - lookback` and copies them
again, in the same transaction, so a recomputed or deleted row inside the window
converges. With both set, the window never reaches below the cutoff.

### Views

A relation that is a view on the source is not copied as data. It is recreated on
the target from the source's definition, after every table and after the views it
reads. All the relations a view reads must be in the same profile, or the run is
refused before any data moves. A target relation with the view's name that is not a
view is refused too: drop it by hand once.

### Schema drift

Before copying, the target table is aligned to the source: missing columns are
added, extra ones dropped, changed types altered. Each change is attempted on its
own, and one that fails is a `WARNING` line, not the end of the run. A column a
view depends on cannot be dropped or retyped — typically a local warehouse that a
migration has taken ahead of the source. Such an extra column is kept and made
nullable, so the reload still lands (with `NULL` in it). For an exact copy, drop
the views the warning names (dbt rebuilds them) and run the sync again.

Each table is applied in one transaction. A failure or a dropped connection rolls
that table back and leaves the others; the next run picks up from the target's
watermark. Every step is idempotent: running twice changes nothing the second time.

## Pulling from a serving source (`dev → local`)

A serving source runs beside dev's Postgres. Its database URL comes from
`SYNC_SOURCE_DSN`, never from the command line; it reads in a read-only session, only
inside its own schema allowlist, and refuses to start on an unmarked database. It
answers a pull on an edge that starts at its own marker and pulls, nothing else.

```bash
SYNC_SOURCE_DSN=… sync-tables serve \
  --listen 0.0.0.0:8443 --health-port 8081 \
  --allow-schemas /etc/sync/allow.txt \
  --token-file /etc/sync/tokens/ \
  --tls-cert /etc/sync/server.crt --tls-key /etc/sync/server.key
```

`--token-file` is a file (one token) or a directory with one file per holder, named after
it. The directory is re-read every few seconds: removing a file revokes that holder without
a restart, and the log of an accepted connection names the holder, never the token.

A serving source runs on server TLS alone when the network it is reachable from is the
gate (an internal-only host): each developer holds a personal token and trusts the pull
CA's certificate for the host. Add `--client-ca` to require client certificates as well.
Behind an ingress, `--tls-terminated-upstream`. Use a CA for pulls that is not the CA of a
push (`certs ca --purpose pull` and `--purpose push`, in separate directories). The local
sink checks its own `celine:env=local` marker before it dials, and writes only the
profile's schemas.

## Pushing to a sink (`dev → prod`)

A sink runs beside the target database. Its database URL comes from `SYNC_SINK_DSN`,
never from the command line, and it refuses to start on an unmarked database.

```bash
SYNC_SINK_DSN=… sync-tables receive \
  --listen 0.0.0.0:8443 --health-port 8081 \
  --allow-schemas /etc/sync/allow.txt \
  --token-file /etc/sync/tokens/ \
  --tls-cert /etc/sync/server.crt --tls-key /etc/sync/server.key --client-ca /etc/sync/ca.crt
```

A sink always requires a client certificate from the push CA. Behind an ingress that
terminates TLS and verifies it, use `--tls-terminated-upstream` with
`--require-upstream-client-cert HEADER` and `--client-ca`: the ingress forwards the
certificate it verified (`X-Forwarded-Tls-Client-Cert` from Traefik's `passTLSClientCert`,
`ssl-client-cert` from ingress-nginx's `auth-tls-pass-certificate-to-upstream`), and the
sink checks it again against the push CA. A request without one is refused
(`client_cert_unverified`), so an ingress that stops verifying fails every push instead of
opening the route. The sink refuses to start behind an ingress without that check.
The sink serves one route, `/sync/v1`, and `/healthz` on the separate health port.

The source pushes with the edge, a `wss://` URL and its client material (each also as
an environment variable: `SYNC_SINK_URL`, `SYNC_TLS_CA`, `SYNC_TLS_CERT`,
`SYNC_TLS_KEY`, `SYNC_TOKEN_FILE`):

```bash
sync-tables --from dev --to prod --profile /etc/sync/profile.yaml \
  --sink-url wss://sink.example.org --ca ca.crt --cert client.crt --key client.key --token-file token
```

The source trusts only the push CA for the sink's certificate, and the sink accepts
only client certificates from that CA, plus the token. A `403` from the sink's ingress
address allowlist is reported as `source_refused`. The prod profile is not in this
repository. It is mounted at run time.

### `sync-tables certs`

Generates the private mTLS material. Files are PEM, keys are mode 0600, nothing secret
is printed, and an existing file is kept unless you pass `--force`.

```bash
sync-tables certs all --out ./pki-push --purpose push --dns sink.example.org --name source
sync-tables certs client --out ./pki-push --name another-client
```

### Failed token attempts

A listener locks out a client after repeated wrong tokens (`429`, `rate_limited` on the
client side): by default 5 failures within 60 s lock it out for 5 minutes, and 50 across
all clients refuse everyone until the window drains. See `--auth-*` in `serve --help`
and [`docs/operations.md`](docs/operations.md#failed-attempt-limit).

## Local tasks

`taskfile.yaml` holds the generic tasks (`task test`, `task test:pg`, `task lint`, `task check:public`,
`task run:pull CONFIG=<name>`). A deployment that uses the tool keeps its own tasks (its
named configs, a first-time setup, a check against its serving source) in a
`taskfile.local.yml` beside it. That file is gitignored and included with
`optional: true, flatten: true`, so its tasks appear without a prefix and the repository
works without it.

## Logs

Log lines carry table names, row counts, watermarks, durations and refusal codes.
They never carry a connection URL, host, credential, certificate subject, client
address or row value. Database errors are reduced to their class, SQLSTATE and the
object names Postgres reports.

## Image

`Dockerfile` builds every role into one image, with code only: no `config.yaml`, no
named configs, no `.env`, no certificates, no profile. Mount a profile and pass
`--profile`. It runs as uid 10001, so
mounted keys and the token must be readable by that user. `version.txt` holds the
version; the image tag is the version.

A change to `version.txt` on `main` releases: `.github/workflows/release.yaml` runs the
tests, then builds and pushes `ghcr.io/celine-eu/sync-tables:<version>`. An existing tag
is never overwritten; bump `version.txt` to release again.

## Tests

```bash
task test          # SQLite and an in-process loopback; the Postgres tests skip
task test:pg       # everything, against two throwaway local PostGIS containers
task lint          # ruff, the rule set in pyproject.toml
task check:public  # the tree holds no keys, tokens, real hosts or addresses
```

The Postgres tests need two clusters (source and target must have different system
identifiers): `SYNC_TABLES_TEST_PG_URL` and `SYNC_TABLES_TEST_PG_URL_2`. They skip a
database that carries a marker other than `local`. Never point them at a shared
database.

`task check:public` (and CI) checks structural patterns only: private keys, certificate
bodies, tokens, addresses outside the loopback and documentation ranges, hostnames and
e-mail addresses outside the `example.*` domains, environment-prefixed warehouse schemas
and personal identifiers. `NAMES=<file>` adds a list of reserved names kept outside the
repository; a finding is reported as `path:line: label`, never with the matched text.

## License

Apache License 2.0, see [`LICENSE`](LICENSE).
