# Architecture

## Environments and edges

An environment is a name, one lowercase word (`[a-z][a-z0-9-]*`), carried by each
database's marker. A copy runs along an **edge**, from one environment to another, with
one or more transports. Typical edges:

| Edge | Transports | Typical use |
|---|---|---|
| `dev → local` | pull, direct | a developer's machine gets a copy of the dev warehouse |
| `dev → prod` | push | production gets a mirror of selected dev tables |
| `dev → staging`, `dev → preprod` | push | read-only copies that mirror production |

**One rule is compiled in:** a sink marked `prod` accepts only a source marked `dev`
(`prod_only_from_dev`). No configuration can change it.

**Every other edge is declared twice**, once by each side, and a copy needs both:

- the side that dials (a run, a relay) lists the edge and its transports in the `edges` of
  its config file: `- {from: dev, to: local, transports: [pull, direct]}`;
- the side that listens names the other end: a sink the sources it accepts
  (`receive --accept-from dev`), a serving source the targets it serves
  (`serve --serve-to local`).

So no single file opens an edge, and none opens one into prod from anything but dev. An
undeclared edge is refused with `edge_not_allowed` before any connection is opened, and a
transport the edge does not declare with `transport_mismatch`.

## Roles

A run always has two roles:

- **Source**: reads its own database and sends what the target is missing. It only ever
  reads, in a read-only session.
- **Sink**: holds the target database, checks every guard against its own database, and
  applies what it receives, one transaction per table.

Rows always flow from the source to the sink, whichever side opens the connection.

## Transports

The transport follows from the arguments: `--source-url` pulls, `--sink-url` pushes,
neither copies directly. An edge refuses a transport it does not list.

```
direct   one process: Source ──loopback──► Sink        (both databases in reach)

push     source host                       target host
         Source ──── WSS, mTLS + token ───► sync-tables receive ──► target DB
         (dials)                            (listens)

pull     developer machine                 dev host
         Sink ◄──── WSS, mTLS + token ──── sync-tables serve ◄── dev DB (read-only)
         (dials)                            (listens)
```

- **Direct** runs both roles in one process over an in-memory channel that carries the
  same bytes as the network. It needs both connection URLs.
- **Push** fits a target that holds its own credentials: the sink runs next to the
  target database and is the only thing that can write to it.
- **Pull** fits a target that cannot be reached, such as a laptop behind NAT: the local
  sink dials a serving source next to the dev database. The laptop never holds dev
  database credentials.

## The wire protocol

Messages are msgpack maps with a type field `t`. Rows travel column by column, and dates,
decimals, UUIDs and geometries use fixed binary extension types. A frame is at most 16 MB.
The set of messages is closed:

```
source → sink   offer   {v, edge, env, sysid, tables, views}
sink → source   accept  {env, sysid, watermarks}          | refuse {code, message}
per table:
source → sink   table   {desc, plan}
source → sink   rows    {columns}                         (zero or more)
sink → source   ack                                       (one per rows, once applied)
source → sink   end                                       | abort {code}
sink → source   ok      {rows}                            | err {code, message}
per view, after every table:
source → sink   view    {schema, name, body}
sink → source   ok | err
source → sink   bye
```

After every table and view, a sink that advertised `caps: ["events"]` in its `accept`
can receive the run events the push carries (see *Run events* below):

```
source → sink   events  {items}                           (once, only when nothing failed)
sink → source   ok      {published}                       | err {code, message}
```

A sink of an earlier release sends no `caps` and gets no `events` message, so a newer
source and an older sink still agree: the protocol version stays 2.

On a pull the sink dials, so it opens each connection with a `request` naming the edge,
the tables it wants and the phase (`plan` or `run`). The serving source answers with a
plan, or with the same `offer` as above.

**Flow control (protocol version 2, release 0.5.0).** The source keeps at most four `rows`
frames unacknowledged, and reads the acks it is still owed before `ok`/`err` and after an
`abort`. Without it, a sink that writes slower than its source reads (a large table, a small
target) let frames pile up until the WebSocket library stopped reading the socket, keepalive
pings included, and the source dropped the connection about 40 s into the table. A peer on
protocol 1 is refused with `protocol_version`: the source and the sink upgrade together.

**The first batch before `table` (release 0.5.1).** The source reads in autocommit, so its
first batch arrives only once the whole result has been fetched. It reads that batch before
sending `table`: the sink opens its transaction (and truncates, on a full sync) only once
rows are ready. Before, a read longer than the target's `idle_in_transaction_session_timeout`
(30 s in staging and prod) left the sink idle in its transaction until Postgres killed it.
No message changed: the protocol stays at version 2.

No message carries SQL except a view body (see [security.md](security.md#view-bodies)).
A table travels as a description (column names, type names, nullability, primary key,
indexes with their access method), and the sink builds every statement itself.

## How a table is copied

Each table in a profile is either incremental or full:

| Profile entry | What the source sends | What the sink does |
|---|---|---|
| no `date_column` | every row | truncate, insert |
| `date_column` | rows newer than the target's `max(date_column)` | upsert on the primary key |
| `+ cutoff: 15d` | never rows older than `now - cutoff` | as above |
| `+ lookback: 2d` | rows from `watermark - lookback` | delete that window, then insert it again |
| `--full-refresh` | every row (within the cutoff) | truncate first |

Before the rows, the sink aligns the target table with the source description:

1. creates the table when it is missing (optional);
2. adds missing columns, drops extra ones, changes types. Each change runs in its own
   savepoint, and a change that fails (a column a view depends on, for example) is a
   warning, not the end of the run;
3. creates missing indexes with the source's access method (`btree`, `hash`, `gist`,
   `spgist`, `gin`, `brin`): a GiST index on a geometry stays GiST. The source leaves out
   expression indexes, indexes with another access method and indexes with a non-default
   operator class, each with a log line. A SQLite target builds btree indexes only.

All of it, the rows included, is one transaction per table. A dropped connection or an
error rolls that table back and leaves the others. The next run starts again from the
target's watermark, so running twice changes nothing the second time.

## Views

A relation that is a view on the source is not copied as data. After every table, the
source sends the view's definition, read with an empty `search_path` so that every name
in it is schema-qualified. The sink recreates it with `CREATE OR REPLACE VIEW`. Views are
sent after the views they read, and a view whose base relations are not all in the
profile refuses the run before any data moves.

## Environment markers

Each database says which environment it is, as a comment on the database:

```sql
COMMENT ON DATABASE warehouse IS 'celine:env=dev';
```

Both roles read the marker of their own database and refuse to run when it is missing,
malformed or does not match the edge. The tool never writes a marker: the database owner
sets it once, ideally in the deployment that creates the database.

## Run events and the relay

The pipelines' flows report each run as a `PipelineRunEvent` (`started`, `completed`,
`failed`). Services react to `completed` on `celine/pipelines/runs/<namespace>`, and they
must see it **after** the data it announces has reached their environment. So the events
travel with the push:

```
flow ── HTTP POST /events ──► relay (one per environment, no token: the network is its boundary)
                                │ keeps `completed`, answers 202
                                ▼ one push loop
                              push: tables, views, then events {items}   (only if every one settled ok)
                                ▼
                              sink: publishes each item unchanged on
                                    celine/pipelines/runs/<namespace>, QoS 1, on its broker
```

- **The relay** (`sync-tables relay --config FILE --sink NAME`) serves `POST /events` and
  `GET /healthz`. It validates the body as a run event (`400` otherwise, `413` above
  64 KiB), keeps `completed` events and ignores the others.
- **One push loop per relay.** It pushes when events are pending and `min_interval` has
  passed since the last push started, or when `max_interval` has passed with no push, so
  the tables are mirrored even when no flow runs. The first cycle pushes at once. One push
  at a time; a push carries only the events received before it started.
- **A failed push keeps its events** for the next one, ahead of newer ones. At most
  `pending_max` are kept; above that the oldest are dropped, with a log line. A sink
  without the capability drops them, with a log line.
- **The sink publishes** each item unchanged with the `events` extra (celine-sdk), on the
  broker and with the client named by `CELINE_MQTT_*` and `CELINE_OIDC_*`. A publish
  failure is logged and the push stays `ok`: the tables are committed already.
- **Delivery is at most once.** A lost event is not replayed; listeners that need every run
  check on their own timer.
- **`local` mode** (`relay --local`), for a development stack where flows and services
  share one database: no data push, each pending event is published directly.
