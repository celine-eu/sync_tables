# Security

## What is being protected

| Asset | Threat |
|---|---|
| The production database | anything other than the mirrored tables being written; data flowing from prod back to dev; a dev-side compromise spreading into prod |
| The dev warehouse | being written by a sync; being read beyond the schemas meant to be shared |
| A developer's local database | being overwritten by a sync aimed elsewhere; a hostile source running code in it |
| Credentials | database URLs, keys and tokens reaching logs, command lines, images or the wrong machine |

The model assumes that **dev is less trusted than prod**: more people and more
automation can touch it. A guard therefore never relies on what the other side says
about itself when it can check the fact on its own side.

## Guards

### 1. Edges are compiled in

Only `dev → local` (pull or direct) and `dev → prod` (push) exist. Any other pair,
including the reverse of each, fails with `edge_not_allowed` before a connection is
opened. A transport the edge does not list fails with `transport_mismatch`.

*Stops:* a copy running backwards, in either direction, through a typo or a misconfigured
job.

### 2. Each database states its environment

Each side reads its own database's comment (`celine:env=<env>`) with a fully qualified
catalog query, so neither `search_path` nor a connection option can redirect what is
read. A missing, malformed or wrong marker refuses the run (`marker_missing`,
`marker_malformed`, `marker_mismatch`). The listeners refuse to start on a database with
no marker. The serving source checks its own marker again on every request.

*Stops:* a URL pointing at the wrong database, such as a "local" URL that is really
production.

### 3. Source and target are different clusters

Each side reports the cluster's `system_identifier`. The same identifier on both sides
fails with `same_cluster`, whatever the markers say.

*Stops:* a copy onto itself, for example two databases in one cluster marked differently.

### 4. The source only reads

- The session starts with `default_transaction_read_only=on`, and then
  `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` runs as well.
- A serving source reads only the schemas in its own allowlist (`schema_not_allowed`).
  It does not take the edge, the table list or the windows in a request on trust: each is
  validated, and a request names at most 1000 tables.

**Deploy the source with a role that can only read** (`USAGE` and `SELECT`). The session
setting is a second line of defence; the role's privileges are what make reading only a
guarantee.

### 5. The sink trusts nothing about its own database

- It reads its own marker and cluster rather than taking the source's word.
- It writes only to its schema allowlist. For `dev → local` that is the profile's
  schemas; a listening sink has its own allowlist, mounted at run time.
- It quotes table, column and index names as identifiers and sends values as bound
  parameters. An index's access method is a word from a closed list (`btree`, `hash`,
  `gist`, `spgist`, `gin`, `brin`); anything else refuses the table description.
- **A column type must be a plain type name** (`catalog.check_type_sql`): one name,
  optionally schema-qualified or quoted, then optional modifiers (`(10, 2)`,
  `(POINT,4326)`), an optional `with[out] time zone` and array brackets. Nothing else is
  accepted. See [Types](#column-types) for why this has to be strict.
- A view body runs as a single prepared statement, so it cannot carry a second statement.
  A view only replaces a view: a relation with the same name that is not a view is
  refused. Views are dropped without `CASCADE`.

**Deploy the sink with a role that owns only the mirrored schemas**, not a superuser and
with no `CREATE` on the database.

### 6. Network: TLS, certificates and tokens

Both edges run over WebSocket on TLS 1.2 or later. The dialling side trusts one private
CA alone (no public CAs) for the listener's certificate and checks the host name. There
is one CA per purpose (`sync-tables certs ca --purpose pull|push`). Keys are EC P-256. A
CA cannot sign intermediates (`path_length=0`). Server and client certificates carry
separate extended key usages.

**Push (`dev → prod`): mutual TLS, an address allowlist and a token.** The edge reaches
production, so every safeguard is required:

- The sink, or the ingress in front of it, requires a client certificate from the push CA.
- Behind an ingress, the sink never sees the handshake, so it cannot tell an ingress that
  verified a certificate from one that verified nothing. The ingress therefore forwards
  the certificate it verified, and the sink checks it again against the push CA: issued
  directly by it, within its validity, for client authentication
  (`--require-upstream-client-cert`). A request without one is refused with
  `client_cert_unverified`. **An ingress that stops verifying makes every push fail
  instead of silently opening the route.** The sink refuses to start behind an ingress
  without this check.
- The ingress admits only the source's egress address. A change of address fails every
  push with `source_refused`, which an alert on the push job reports.
- A token, checked as described below.

**Pull (`dev → local`): server TLS, a private network and a personal token.** The
serving source answers only on a host reachable from a private network (an office network
or a VPN). That network is the gate. Each developer holds a personal token and trusts the
pull CA's certificate (which is public) for the host. No client certificate is needed. A
serving source can still require one (`--client-ca`) where the host is reachable from a
wider network.

**Tokens.** A listener accepts one token per holder, at least 32 characters each,
compared in constant time before the WebSocket opens. The tokens are kept in a directory
with one file per holder, named after it, and re-read every few seconds: removing a file
revokes that holder without a restart. The log of an accepted connection names the holder
(a label the operator chose), never the token. On the client, a token comes from a file or
from `SYNC_TOKEN` in the environment, never from the command line.

**The rest of the listener:**

- One route, `/sync/v1`; anything else gets a 404. The health check is served on a
  separate plain-HTTP port that should not be exposed.
- Incoming data is bounded: frames are at most 16 MB, an undecodable frame is refused,
  only known extension types are decoded, and text frames are rejected.

### 7. Failed token attempts are limited

A listener counts failed token checks per client. With the defaults, 5 failures within
60 seconds lock that client out for 5 minutes. 50 failures within 60 seconds, across all
clients, refuse every request until the window drains. A refused request gets
`429 Too Many Requests` with `Retry-After` before its token is even looked at. The client
reports `rate_limited`. See [operations.md](operations.md#failed-attempt-limit) for the
settings and for how the client is identified behind an ingress.

The limit is defence in depth, not the main protection. A 48-character random token
cannot be guessed. What the limit adds is that guessing gets very few tries, and that
repeated failures stand out in the logs.

Behind NAT, every client arrives from the same address, so a per-client lockout locks
out everyone behind it. Keep the lockout short on such a listener.

### 8. Credentials stay where they belong

- Listener database URLs come only from the environment (`SYNC_SOURCE_DSN`,
  `SYNC_SINK_DSN`), never from the command line.
- On a pull, the developer's machine holds a client certificate and the token, never
  database credentials for dev. On a push, the target's credentials stay next to the
  target.
- The image holds code only: no configuration, profile, certificate or token. It runs as
  an unprivileged user.

### 9. Logs carry no secrets and no data

Log lines carry table names, row counts, watermarks, durations and refusal codes. They
never carry:

- a connection URL, host or credential;
- a certificate subject, a client address or a token (a token's holder name is logged
  when a connection is accepted);
- a row value or a SQL statement.

Database errors are reduced to their class, SQLSTATE code and the object names Postgres
reports. There are no tracebacks.

## Limits that remain

### Column types

The column types in a table description go into `CREATE TABLE` and `ALTER TABLE`. An
earlier, looser check allowed letters, spaces, commas, dots and parentheses. That was
enough to smuggle in a clause:

- `text DEFAULT f(...)`: `ADD COLUMN` with a volatile default evaluates `f` once for
  every existing row;
- `int CHECK (f(...) IS NULL)`: evaluated on every insert;
- `text, extra int`: an extra column;
- `concat(chr(47), chr(101), ...)`: builds any string without a quote character.

How much that matters depends on the sink's role. A non-superuser mirror role exposes
little beyond stalling the sink (`pg_sleep` while it holds locks) or leaving expressions
behind in the mirrored tables. A **superuser** target is different: file functions
(`pg_read_file`, `lo_from_bytea` with `lo_export`) and `pg_reload_conf` can be chained
into code execution on the database host. Local development databases often run as the
superuser.

The current grammar admits a type name and its modifiers only, and a test checks it
against every type a real Postgres catalog reflects. Keep the sink's role
non-superuser anyway.

### View bodies

A view body is the one piece of SQL a sink accepts. It is limited to one statement, but
within that statement it is any `SELECT`. Creating a view runs nothing. Querying it runs
whatever functions the body calls, as the user who queries it, and reads relations with
the view owner's privileges.

A compromised source can therefore plant a view that does harm when someone later reads
it on the target. Mitigations:

- the sink role is not a superuser, and owns only the mirrored schemas;
- the applications that read the target do not run as a superuser;
- a profile lists a view only when a reader on the target needs it;
- a developer's local sink does not run as a superuser (see
  [operations.md](operations.md#local-databases)).

### Pull: the network and the token are the gate

On a pull, the target's environment and cluster are reported by the client that dials.
The serving source cannot check them. Anyone on the private network who holds a valid
token can therefore read every allowlisted schema. The controls are:

- network membership: offboarding removes network or VPN access;
- the personal token: revoke the holder's file;
- a narrow schema allowlist.

A token is a bearer secret. Copied together with network access, it works for whoever
holds it. Nothing ties it to a device, and the logs name the holder whose token was
used, which is not necessarily the person who used it.

The perimeter is only as good as the network configuration. If the ingress that serves
the internal host name also listens on a public address, a request to that address with
the internal `Host` header reaches the listener. Verify that the internal host is
unreachable from outside ([operations.md](operations.md#checks)).

### TLS behind an ingress

With `--tls-terminated-upstream`, client-certificate checking is the ingress's job. On a
sink, the forwarded-certificate check catches an ingress that stopped verifying. A
serving source that requires client certificates but has no such check would be left
with only the token. Verify the ingress after every change
([operations.md](operations.md#checks)).

The forwarded-certificate check proves that a certificate from the push CA was presented
to the ingress, provided the ingress replaces the header rather than passing on a
client-supplied one. Both Traefik's `passTLSClientCert` and ingress-nginx set the header
themselves. The certificate is not secret in the way a key is, so the check is a safeguard
against a misconfigured ingress, not a replacement for one that verifies.
