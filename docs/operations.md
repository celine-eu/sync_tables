# Operations

Host names below are examples (`*.example.org`). Use your own.

## Database roles

| Role | Where | Needs | Must not have |
|---|---|---|---|
| source reader | dev | `USAGE` on the shared schemas, `SELECT` on their tables (and the default privilege for future tables) | any write privilege, superuser |
| mirror writer | prod | ownership of the mirrored schemas (or `CREATE` in them, plus ownership of their tables) | superuser, `CREATE` on the database, access to other schemas |
| local writer | developer machine | ownership of the synced schemas | ideally superuser (see below) |

Mark each database once, as its owner, ideally from the deployment that creates it:

```sql
COMMENT ON DATABASE warehouse IS 'celine:env=dev';    -- or prod, local
```

### Local databases

A developer's local database often runs everything as the `postgres` superuser. A sink
running as a superuser gives a compromised dev side more power than it needs (see
[security.md](security.md#limits-that-remain)). Prefer a dedicated local role that owns
the synced schemas, and point `DB_LOCAL` at it.

## Certificates and tokens

`sync-tables certs` writes PEM files, with keys and tokens at mode 0600. It never prints
a secret and never overwrites a file without `--force`. Keep one directory per purpose,
outside any repository:

```bash
# Pull: developers dial a serving source on an internal host. A server certificate only.
sync-tables certs ca     --out ./pki-pull --purpose pull
sync-tables certs server --out ./pki-pull --dns sync-source.internal.example.org

# Push: dev's source dials a sink next to prod. Mutual TLS.
sync-tables certs ca     --out ./pki-push --purpose push
sync-tables certs server --out ./pki-push --dns sync.example.org
sync-tables certs client --out ./pki-push --name source
sync-tables certs token  --out ./pki-push
```

- The pull CA's **certificate** is public. Ship it to developers, for example in the
  repository that holds their sync configs. Its key stays with the operator.
- The push CA's certificate goes to the sink (or its ingress), and the push client
  material goes to the push source only. No person holds it.
- **Never configure a listener to trust both CAs.**
- Keep the CA keys offline, or in an encrypted secret store. A listener needs only the CA
  certificate.

Leaf certificates last 397 days by default, and the CA 10 years.

### Tokens

A listener reads its tokens from `--token-file`: a file (one holder) or a directory with
one file per holder, named after it (lower-case, up to 64 characters). Each token is at
least 32 characters, and no two holders may share one. A mounted Kubernetes Secret with
one key per holder is such a directory.

```bash
# a token for one developer
openssl rand -base64 48 | tr -d '\n/+=' | cut -c1-48 > tokens/alice
```

The directory is re-read every 10 seconds: adding a holder takes effect, and removing one
revokes it, without a restart. A developer puts their token in `.env` as `SYNC_TOKEN`.

## Running the listeners

```bash
# Serving source, next to dev (read-only role), on an internal-only host
SYNC_SOURCE_DSN=… sync-tables serve \
  --listen 0.0.0.0:8443 --health-port 8081 \
  --allow-schemas /etc/sync/allow.txt --token-file /etc/sync/tokens/ \
  --tls-cert server.crt --tls-key server.key

# Sink, next to prod (mirror role)
SYNC_SINK_DSN=… sync-tables receive \
  --listen 0.0.0.0:8443 --health-port 8081 \
  --allow-schemas /etc/sync/allow.txt --token-file /etc/sync/tokens/ \
  --tls-cert server.crt --tls-key server.key --client-ca push-ca.crt
```

### Behind an ingress

With `--tls-terminated-upstream`, the listener serves plain WebSocket. The ingress must:

- present the server certificate signed by **the purpose's CA**, not a public
  certificate, because clients trust that CA alone;
- route only the exact path `/sync/v1` to the listener;
- allow long-lived connections: a table is one transaction and can take minutes, so set
  the proxy read and send timeouts to about an hour;
- allow bodies of at least 16 MB;
- overwrite `X-Real-IP` / `X-Forwarded-For` with the real client address (see below).

For a **sink**, it must also:

- require a client certificate from the push CA, at verify depth 1;
- admit only the source's egress address;
- forward the certificate it verified, and the sink runs with
  `--require-upstream-client-cert HEADER --client-ca push-ca.crt`.

| Ingress | Client certificate | Address allowlist | Forwarded certificate header |
|---|---|---|---|
| Traefik (native) | `TLSOption` with `clientAuth.clientAuthType: RequireAndVerifyClientCert` and the push CA, `sniStrict: true` | `ipAllowList` middleware | `passTLSClientCert` middleware with `pem: true`: `X-Forwarded-Tls-Client-Cert` |
| ingress-nginx | `auth-tls-secret`, `auth-tls-verify-client: "on"`, `auth-tls-verify-depth: "1"` | `whitelist-source-range` | `auth-tls-pass-certificate-to-upstream: "true"`: `ssl-client-cert` |

On Traefik, use the native resources even where its NGINX-compatibility provider is
enabled, rather than relying on that provider to translate the client-certificate
annotations. The address allowlist only works when the ingress sees the client's real
address, for example a load balancer with `externalTrafficPolicy: Local`.

For a **serving source on an internal host**, the ingress serving that host must not be
reachable from outside the private network, including by sending the internal `Host`
header to a public address.

A network policy should let only the ingress reach the listener port, and only the
database on 5432 be reached from the listener. The health port stays inside the cluster.

Keep the ingress controller patched. A listener's security behind an ingress is only as
good as that controller.

## Failed-attempt limit

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--auth-fail-max` | `SYNC_AUTH_FAIL_MAX` | 5 | failures from one client within the window that lock it out |
| `--auth-fail-window` | `SYNC_AUTH_FAIL_WINDOW` | 60 | seconds the failures are counted over |
| `--auth-lockout` | `SYNC_AUTH_LOCKOUT` | 300 | seconds a client stays locked out |
| `--auth-fail-global-max` | `SYNC_AUTH_FAIL_GLOBAL_MAX` | 50 | failures from all clients within the window that refuse everyone |

A client is its address:

- when TLS ends at the listener, the socket's peer address;
- with `--tls-terminated-upstream`, `X-Real-IP`, then the first `X-Forwarded-For`
  entry, as set by the ingress.

The header is only meaningful because the network policy lets nothing but the ingress
reach the pod, and the ingress overwrites those headers. Developers behind one NAT share
an address and a lockout. The state is in memory, per replica, and resets on restart.
Addresses are never logged. Behind NAT, as with developers pulling over a VPN, one address
stands for everyone: raise `--auth-fail-max` (around 20) and shorten `--auth-lockout`
(around 60 s), so one person's typo does not lock the team out for long.

The ingress can add its own request rate limit in front, but such a limit counts every
request, not only failed ones.

## Rotation

**A developer's token.** Replace or remove their file in the token directory; it takes
effect within seconds. Do this when someone leaves (together with removing their network
access) or reports a leak. Others are not affected.

**The push material** (when it may have leaked, and before the certificates expire):

1. issue a new client certificate and token (`--force` replaces files in place; a new CA
   only when the CA key itself is in doubt);
2. update the sink's token file (and CA certificate, if replaced);
3. update the push source's secret.

Until both sides match, pushes fail with `auth_failed`, or with `client_cert_refused` when
the CA changed, and the push alerts fire.

**The serving host's certificate** before it expires, or when the host name changes:
issue a new server certificate from the pull CA. Developers need nothing new unless the
CA itself changes.

## Checks

After deploying or changing a listener or its ingress:

| Check | Expected |
|---|---|
| sink: request without a client certificate | refused by TLS or the ingress |
| sink: client certificate from another CA (for example the pull CA) | refused |
| sink: a request from an address outside the allowlist | `403`, reported as `source_refused` |
| sink: the ingress forwarding no certificate (test with the check off at the ingress, never in production) | `403`, `client_cert_unverified` |
| serving source: the internal host from outside the private network, also with the internal `Host` header sent to a public address | no connection |
| right certificate (where required), wrong token | `401`, and a `REFUSED [auth_failed]` log line |
| right token | the log names the holder: `accepted: token holder <name>` |
| repeated wrong tokens | `429` with `Retry-After` |
| any path other than `/sync/v1` | `404` |
| plain HTTP | redirect to HTTPS, or refused |
| server certificate | issued by the purpose's sync CA |
| the source role writing anything | `permission denied` |
| the sink role writing outside its schemas | `permission denied` |

## Exit codes and refusal codes

Exit codes: `0` everything copied, `1` some tables failed (the others were copied),
`2` the run was refused or could not start.

| Code | Meaning |
|---|---|
| `edge_not_allowed`, `unknown_environment`, `transport_mismatch` | the edge or transport is not one the tool has |
| `marker_missing`, `marker_malformed`, `marker_mismatch` | a database's environment marker is absent or wrong |
| `same_cluster` | source and target are the same cluster |
| `schema_not_allowed` | a table is outside an allowlist |
| `view_base_missing`, `view_name_taken` | a view cannot be recreated safely |
| `auth_failed`, `client_cert_refused`, `tls_failed`, `handshake_failed` | the connection was not authenticated |
| `client_cert_unverified` | the sink's ingress forwarded no client certificate from the push CA: it is not checking them |
| `source_refused` | the sink's ingress refused the source's address: has the egress address changed? |
| `rate_limited` | too many failed attempts from this client, or from all clients |
| `bad_message`, `bad_request`, `protocol_version` | a malformed or incompatible message |
