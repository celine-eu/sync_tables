from __future__ import annotations

import argparse
import os
import ssl
import sys
import time

from dotenv import load_dotenv

from sync_tables.config import list_configs, load_config, load_profile
from sync_tables.envs import DIRECT, PULL, PUSH, pick_transport
from sync_tables.log import Refused, SyncError, log, safe_error

EXIT_OK, EXIT_TABLES_FAILED, EXIT_REFUSED = 0, 1, 2

USAGE = """\
sync-tables [CONFIG] --from dev --to local --source-url wss://… --ca F --token-file F [--cert F --key F] [--full-refresh]
sync-tables [CONFIG] --from dev --to local [--profile FILE] [--full-refresh]   (direct: reads DB_DEV itself)
sync-tables [CONFIG] --from dev --to prod|staging|preprod --sink-url wss://… --ca F --cert F --key F --token-file F [--profile FILE]
sync-tables receive …   (run a sink; see `sync-tables receive --help`)
sync-tables serve …     (run a serving source for pulls; see `sync-tables serve --help`)
sync-tables certs …     (generate the sync's mTLS material; see `sync-tables certs --help`)
sync-tables --list
"""


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["certs"]:
        from sync_tables.certs import main as certs_main  # noqa: PLC0415

        return certs_main(argv[1:])
    if argv[:1] == ["receive"]:
        return receive_main(argv[1:])
    if argv[:1] == ["serve"]:
        return serve_main(argv[1:])
    return sync_main(argv)


# ---------------------------------------------------------------------------
# A sync run: the source side
# ---------------------------------------------------------------------------

def sync_main(argv: list[str]) -> int:
    load_dotenv(override=False)  # before the parser: .env may carry SYNC_SOURCE_URL and the client material
    available = list_configs("./config.yaml")
    epilog = f"available configs: {', '.join(available)}" if available else None
    env = os.environ.get
    p = argparse.ArgumentParser(prog="sync-tables", usage=USAGE, description="Copy tables along an allowed edge", epilog=epilog)
    p.add_argument("config_name", nargs="?", default=None, help="named config from config/ next to config.yaml (e.g. 'orders')")
    p.add_argument("--from", dest="from_env", help="source environment (dev)")
    p.add_argument("--to", dest="to_env", help="target environment (local, prod, staging, preprod)")
    p.add_argument("--profile", help="a profile file mounted at run time, instead of a named config")
    p.add_argument("--full-refresh", action="store_true", help="truncate target tables and re-sync all rows")
    p.add_argument("--list", action="store_true", help="list the named configs and exit")
    g = p.add_argument_group("over WSS: push (--to prod|staging|preprod) or pull (--to local)")
    g.add_argument("--sink-url", default=env("SYNC_SINK_URL"), help="wss:// URL of the sink to push to [SYNC_SINK_URL]")
    g.add_argument("--source-url", default=env("SYNC_SOURCE_URL"), help="wss:// URL of the serving source to pull from [SYNC_SOURCE_URL]")
    g.add_argument("--ca", default=env("SYNC_TLS_CA"), help="the sync CA, the only CA trusted for the peer [SYNC_TLS_CA]")
    g.add_argument("--cert", default=env("SYNC_TLS_CERT"), help="client certificate; a push needs one [SYNC_TLS_CERT]")
    g.add_argument("--key", default=env("SYNC_TLS_KEY"), help="client key [SYNC_TLS_KEY]")
    g.add_argument("--token-file", default=env("SYNC_TOKEN_FILE"),
                   help="file holding the bearer token [SYNC_TOKEN_FILE]; or the token itself in SYNC_TOKEN")
    a = p.parse_args(argv)

    if a.list:
        print("\n".join(available) if available else "No configs found in config/")
        return EXIT_OK
    if not a.from_env or not a.to_env:
        p.error("name the edge: --from <env> --to <env> (e.g. --from dev --to local)")

    started = time.monotonic()
    try:
        transport = pick_transport(a.from_env, a.to_env, _wanted(a))  # before any connection is opened
        result = _run(a, transport)
    except Refused as r:
        log(str(r))
        return EXIT_REFUSED
    except SyncError as e:
        log(f"ERROR: {e}")
        return EXIT_REFUSED
    except Exception as e:  # never a traceback: SQLAlchemy errors carry statements and values
        log(f"ERROR: {safe_error(e)}")
        return EXIT_REFUSED

    log(
        f"Done {a.from_env} -> {a.to_env} in {time.monotonic() - started:.1f}s: "
        f"{len(result.ok)} ok, {len(result.failed)} failed, {len(result.skipped)} skipped"
    )
    return EXIT_TABLES_FAILED if result.failed else EXIT_OK


def _wanted(a: argparse.Namespace) -> str:
    """The transport follows from the URL given: a sink to push to, a source to pull from."""
    if a.sink_url and a.source_url:
        raise Refused("transport_mismatch", "give --sink-url or --source-url, not both")
    if a.sink_url:
        return PUSH
    if a.source_url:
        return PULL
    return DIRECT


def _run(a: argparse.Namespace, transport: str):
    from sync_tables.db import ensure_postgis, make_engine, make_source_engine  # noqa: PLC0415
    from sync_tables.protocol import loopback_to  # noqa: PLC0415
    from sync_tables.sink import Sink  # noqa: PLC0415
    from sync_tables.source import Source  # noqa: PLC0415

    url = a.sink_url if transport == PUSH else a.source_url
    if transport != DIRECT and not (url or "").startswith("wss://"):
        peer = "a sink; give --sink-url" if transport == PUSH else "a serving source; give --source-url"
        raise Refused("transport_mismatch", f"{a.from_env} -> {a.to_env} {transport}s over WSS from {peer} wss://…")

    cfg = load_config("./config.yaml", config_name=None if a.profile else a.config_name)
    specs = load_profile(a.profile) if a.profile else cfg.sources
    label = a.profile and os.path.basename(a.profile) or a.config_name or "config.yaml"
    if not specs:
        log(f"No sources in '{label}'. Use --list to see available configs.")
        return _empty()
    log(f"Edge: {a.from_env} -> {a.to_env} ({transport}); profile: {label} ({len(specs)} table(s))")
    if a.full_refresh:
        log("Running in FULL REFRESH mode — every target table is truncated and reloaded")

    if transport == PULL:
        return _pull(a, cfg, specs)
    if not cfg.source_db_url:
        raise SyncError("the source connection URL is not set (source_db.url in config.yaml)")
    src_engine = make_source_engine(cfg.source_db_url, pool_size=cfg.parallelism + 1)
    source = Source(src_engine, (a.from_env, a.to_env), batch_size=cfg.batch_size)

    if transport == DIRECT:
        if not cfg.destination_db_url:
            raise SyncError("the target connection URL is not set (destination_db.url in config.yaml)")
        tgt_engine = make_engine(cfg.destination_db_url, pool_size=cfg.parallelism + 1)
        sink = Sink(tgt_engine, {s.schema for s in specs}, create_table_if_missing=cfg.create_table_if_missing)
        source.check_identity()
        sink.identity_for(a.to_env)  # refuse before touching the target, PostGIS included
        with tgt_engine.connect() as conn:
            ensure_postgis(conn)
        open_channel = loopback_to(sink)
    else:
        from sync_tables.wss import channel_factory, client_tls  # noqa: PLC0415

        _need_client_material(a, "a push")
        open_channel = channel_factory(a.sink_url, client_tls(a.ca, a.cert, a.key), _client_token(a))

    return source.run(specs, open_channel, parallelism=cfg.parallelism, full_refresh=a.full_refresh)


def _client_token(a: argparse.Namespace) -> str:
    """The token to present: SYNC_TOKEN (a developer's .env), or --token-file. Never a
    command-line value, which would show in the process list and the shell history."""
    from sync_tables.wss import read_token  # noqa: PLC0415

    value = os.environ.get("SYNC_TOKEN")
    if value and a.token_file:
        raise SyncError("give the token once: SYNC_TOKEN or --token-file, not both")
    if value:
        if len(value.strip()) < 32:
            raise SyncError("SYNC_TOKEN holds fewer than 32 characters")
        return value.strip()
    return read_token(a.token_file)


def _need_client_material(a: argparse.Namespace, what: str, cert: bool = True) -> None:
    """A push presents a client certificate; a pull may run on server TLS and its token alone."""
    missing = [n for n in ("ca",) + (("cert", "key") if cert else ()) if not getattr(a, n)]
    if not (a.token_file or os.environ.get("SYNC_TOKEN")):
        missing.append("token_file")
    if missing:
        raise SyncError(f"{what} needs {', '.join('--' + m.replace('_', '-') for m in missing)}")


def _pull(a: argparse.Namespace, cfg, specs):
    """dev -> local without reaching dev's database: dial the serving source, apply here."""
    from sync_tables.db import ensure_postgis, make_engine  # noqa: PLC0415
    from sync_tables.pull import pull  # noqa: PLC0415
    from sync_tables.sink import Sink  # noqa: PLC0415
    from sync_tables.wss import channel_factory, client_tls  # noqa: PLC0415

    _need_client_material(a, "a pull", cert=False)
    if not cfg.destination_db_url:
        raise SyncError("the target connection URL is not set (destination_db.url in config.yaml)")
    tgt_engine = make_engine(cfg.destination_db_url, pool_size=cfg.parallelism + 1)
    sink = Sink(tgt_engine, {s.schema for s in specs}, create_table_if_missing=cfg.create_table_if_missing,
                log_summaries=True)
    sink.identity_for(a.to_env)  # refuse before dialling, PostGIS included
    with tgt_engine.connect() as conn:
        ensure_postgis(conn)
    open_channel = channel_factory(
        a.source_url, client_tls(a.ca, a.cert, a.key), _client_token(a), peer="source"
    )
    return pull(specs, (a.from_env, a.to_env), open_channel, sink,
                parallelism=cfg.parallelism, full_refresh=a.full_refresh)


def _empty():
    from sync_tables.source import RunResult  # noqa: PLC0415

    return RunResult()


# ---------------------------------------------------------------------------
# A sink, listening (the prod side of a push)
# ---------------------------------------------------------------------------

def receive_main(argv: list[str]) -> int:
    env = os.environ.get
    p = argparse.ArgumentParser(
        prog="sync-tables receive",
        description="Run a sink: accept pushes over WSS and apply them to this database. "
        "The database URL comes from SYNC_SINK_DSN, never from the command line.",
    )
    p.add_argument("--listen", default=env("SYNC_LISTEN", "0.0.0.0:8443"), help="host:port [SYNC_LISTEN]")
    p.add_argument("--health-port", type=int, default=int(env("SYNC_HEALTH_PORT", "8081")), help="plain-HTTP /healthz port [SYNC_HEALTH_PORT]")
    p.add_argument("--allow-schemas", default=env("SYNC_ALLOW_SCHEMAS_FILE"), help="file listing the schemas this sink may write, one per line [SYNC_ALLOW_SCHEMAS_FILE]")
    p.add_argument("--allow-schema", action="append", default=[], help="a schema this sink may write (repeatable)")
    p.add_argument("--token-file", default=env("SYNC_TOKEN_FILE"),
                   help="a file holding the bearer token, or a directory with one file per holder, named after "
                        "it (re-read every few seconds) [SYNC_TOKEN_FILE]")
    p.add_argument("--tls-cert", default=env("SYNC_TLS_CERT"), help="server certificate [SYNC_TLS_CERT]")
    p.add_argument("--tls-key", default=env("SYNC_TLS_KEY"), help="server key [SYNC_TLS_KEY]")
    p.add_argument("--client-ca", default=env("SYNC_TLS_CA"), help="the push CA, for client certificates [SYNC_TLS_CA]")
    p.add_argument("--tls-terminated-upstream", action="store_true",
                   help="serve plain WS: TLS and client certificates end at the ingress; needs --require-upstream-client-cert")
    p.add_argument("--require-upstream-client-cert", metavar="HEADER", default=env("SYNC_UPSTREAM_CERT_HEADER"),
                   help="behind an ingress: the header it forwards the verified client certificate in "
                        "(X-Forwarded-Tls-Client-Cert for Traefik, ssl-client-cert for ingress-nginx); checked "
                        "again against --client-ca, and a request without one is refused [SYNC_UPSTREAM_CERT_HEADER]")
    _limit_args(p, env)
    p.add_argument("--no-create-tables", action="store_true", help="refuse tables that do not exist yet")
    a = p.parse_args(argv)

    try:
        return _receive(a)
    except Refused as r:
        log(str(r))
        return EXIT_REFUSED
    except SyncError as e:
        log(f"ERROR: {e}")
        return EXIT_REFUSED
    except Exception as e:
        log(f"ERROR: {safe_error(e)}")
        return EXIT_REFUSED


def _receive(a: argparse.Namespace) -> int:
    from sync_tables.db import ensure_postgis, make_engine  # noqa: PLC0415
    from sync_tables.envs import read_identity  # noqa: PLC0415
    from sync_tables.sink import Sink  # noqa: PLC0415
    from sync_tables.wss import make_server, start_health  # noqa: PLC0415

    dsn = os.environ.get("SYNC_SINK_DSN")
    if not dsn:
        raise SyncError("SYNC_SINK_DSN is not set")
    schemas, tokens, tls, upstream = _listener_material(a, mutual=True)

    engine = make_engine(dsn, pool_size=8)
    with engine.connect() as conn:
        me = read_identity(conn, side="target")  # refuse to start unmarked
        ensure_postgis(conn)
    sink = Sink(engine, schemas, create_table_if_missing=not a.no_create_tables, log_summaries=True)

    host, _, port = a.listen.rpartition(":")
    health = start_health(host or "0.0.0.0", a.health_port)
    server = make_server(sink, host or "0.0.0.0", int(port), tokens, tls, limiter=_limiter(a), upstream_cert=upstream)
    return _listen(server, health, f"sink: environment {me.env}, {len(schemas)} allowed schema(s), "
                                   f"{len(tokens.names)} token holder(s), {_tls_line(tls, upstream)}; listening")


# ---------------------------------------------------------------------------
# A serving source, listening (the dev side of a pull)
# ---------------------------------------------------------------------------

def serve_main(argv: list[str]) -> int:
    env = os.environ.get
    p = argparse.ArgumentParser(
        prog="sync-tables serve",
        description="Run a serving source: answer pulls over WSS from this database, read-only. "
        "The database URL comes from SYNC_SOURCE_DSN, never from the command line.",
    )
    p.add_argument("--listen", default=env("SYNC_LISTEN", "0.0.0.0:8443"), help="host:port [SYNC_LISTEN]")
    p.add_argument("--health-port", type=int, default=int(env("SYNC_HEALTH_PORT", "8081")), help="plain-HTTP /healthz port [SYNC_HEALTH_PORT]")
    p.add_argument("--allow-schemas", default=env("SYNC_ALLOW_SCHEMAS_FILE"), help="file listing the schemas this source may read, one per line [SYNC_ALLOW_SCHEMAS_FILE]")
    p.add_argument("--allow-schema", action="append", default=[], help="a schema this source may read (repeatable)")
    p.add_argument("--token-file", default=env("SYNC_TOKEN_FILE"),
                   help="a file holding the bearer token, or a directory with one file per holder, named after "
                        "it (re-read every few seconds) [SYNC_TOKEN_FILE]")
    p.add_argument("--tls-cert", default=env("SYNC_TLS_CERT"), help="server certificate [SYNC_TLS_CERT]")
    p.add_argument("--tls-key", default=env("SYNC_TLS_KEY"), help="server key [SYNC_TLS_KEY]")
    p.add_argument("--client-ca", default=env("SYNC_TLS_CA"),
                   help="a CA for client certificates; without it the listener runs on server TLS alone [SYNC_TLS_CA]")
    p.add_argument("--tls-terminated-upstream", action="store_true", help="serve plain WS: TLS ends at the ingress")
    p.add_argument("--require-upstream-client-cert", metavar="HEADER", default=env("SYNC_UPSTREAM_CERT_HEADER"),
                   help="behind an ingress: the header it forwards the verified client certificate in "
                        "(X-Forwarded-Tls-Client-Cert for Traefik, ssl-client-cert for ingress-nginx); checked "
                        "again against --client-ca, and a request without one is refused [SYNC_UPSTREAM_CERT_HEADER]")
    _limit_args(p, env)
    p.add_argument("--batch-size", type=int, default=int(env("SYNC_BATCH_SIZE", "5000")), help="rows per batch [SYNC_BATCH_SIZE]")
    a = p.parse_args(argv)

    try:
        return _serve(a)
    except Refused as r:
        log(str(r))
        return EXIT_REFUSED
    except SyncError as e:
        log(f"ERROR: {e}")
        return EXIT_REFUSED
    except Exception as e:
        log(f"ERROR: {safe_error(e)}")
        return EXIT_REFUSED


def _limit_args(p: argparse.ArgumentParser, env) -> None:
    g = p.add_argument_group("failed token attempts")
    g.add_argument("--auth-fail-max", type=int, default=int(env("SYNC_AUTH_FAIL_MAX", "5")),
                   help="failures from one client within the window that lock it out [SYNC_AUTH_FAIL_MAX]")
    g.add_argument("--auth-fail-window", type=int, default=int(env("SYNC_AUTH_FAIL_WINDOW", "60")),
                   help="seconds failures are counted over [SYNC_AUTH_FAIL_WINDOW]")
    g.add_argument("--auth-lockout", type=int, default=int(env("SYNC_AUTH_LOCKOUT", "300")),
                   help="seconds a client stays locked out [SYNC_AUTH_LOCKOUT]")
    g.add_argument("--auth-fail-global-max", type=int, default=int(env("SYNC_AUTH_FAIL_GLOBAL_MAX", "50")),
                   help="failures from all clients within the window that refuse everyone [SYNC_AUTH_FAIL_GLOBAL_MAX]")


def _limiter(a: argparse.Namespace):
    from sync_tables.wss import FailedAuthLimiter  # noqa: PLC0415

    if min(a.auth_fail_max, a.auth_fail_window, a.auth_lockout, a.auth_fail_global_max) < 1:
        raise SyncError("the failed-attempt limits must be positive")
    return FailedAuthLimiter(per_client=a.auth_fail_max, window=a.auth_fail_window,
                             lockout=a.auth_lockout, global_max=a.auth_fail_global_max)


def _listener_material(a: argparse.Namespace, mutual: bool):
    """The schema allowlist, the tokens, the TLS context and the upstream certificate check
    a listening role needs. `mutual`: client certificates are required (a push sink), so
    behind an ingress the ingress must prove it checked one."""
    from sync_tables.auth import TokenStore, UpstreamCertCheck  # noqa: PLC0415
    from sync_tables.wss import server_tls  # noqa: PLC0415

    schemas = set(a.allow_schema)
    if a.allow_schemas:
        with open(a.allow_schemas, "r", encoding="utf-8") as f:
            schemas |= {ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")}
    if not schemas:
        raise SyncError("give the schema allowlist (--allow-schemas FILE or --allow-schema)")
    if not a.token_file:
        raise SyncError("give --token-file")
    tokens = TokenStore(a.token_file)

    if a.tls_terminated_upstream:
        upstream = None
        if a.require_upstream_client_cert:
            if not a.client_ca:
                raise SyncError("--require-upstream-client-cert checks against --client-ca; give it")
            upstream = UpstreamCertCheck(a.require_upstream_client_cert, a.client_ca)
        elif mutual:
            raise SyncError(
                "behind an ingress a sink needs --require-upstream-client-cert HEADER and --client-ca: "
                "without them nothing proves the ingress checked a client certificate"
            )
        return schemas, tokens, None, upstream
    if a.require_upstream_client_cert:
        raise SyncError("--require-upstream-client-cert applies behind an ingress (--tls-terminated-upstream)")
    needed = ("tls_cert", "tls_key") + (("client_ca",) if mutual else ())
    missing = [n for n in needed if not getattr(a, n)]
    if missing:
        raise SyncError(
            f"give {', '.join('--' + m.replace('_', '-') for m in missing)}, or --tls-terminated-upstream behind an ingress"
        )
    return schemas, tokens, server_tls(a.tls_cert, a.tls_key, a.client_ca), None


def _tls_line(tls, upstream) -> str:
    if tls is None:
        return "TLS upstream, client certificate re-checked" if upstream else "TLS upstream"
    return "mTLS" if tls.verify_mode == ssl.CERT_REQUIRED else "server TLS"


def _listen(server, health, line: str) -> int:
    log(line)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        health.shutdown()
    return EXIT_OK


def _serve(a: argparse.Namespace) -> int:
    from sync_tables.db import make_source_engine, set_source_read_only  # noqa: PLC0415
    from sync_tables.envs import read_identity  # noqa: PLC0415
    from sync_tables.pull import SourceServer  # noqa: PLC0415
    from sync_tables.wss import make_channel_server, start_health  # noqa: PLC0415

    dsn = os.environ.get("SYNC_SOURCE_DSN")
    if not dsn:
        raise SyncError("SYNC_SOURCE_DSN is not set")
    schemas, tokens, tls, upstream = _listener_material(a, mutual=False)

    engine = make_source_engine(dsn, pool_size=8)
    with engine.connect() as conn:
        set_source_read_only(conn)
        me = read_identity(conn, side="source")  # refuse to start unmarked
    server_role = SourceServer(engine, schemas, batch_size=a.batch_size)

    host, _, port = a.listen.rpartition(":")
    health = start_health(host or "0.0.0.0", a.health_port)
    server = make_channel_server(server_role.serve, host or "0.0.0.0", int(port), tokens, tls, limiter=_limiter(a),
                                 upstream_cert=upstream)
    return _listen(server, health, f"serve: environment {me.env}, {len(schemas)} allowed schema(s), "
                                   f"{len(tokens.names)} token holder(s), {_tls_line(tls, upstream)}; listening")


if __name__ == "__main__":
    raise SystemExit(main())
