"""Edges: one rule compiled in, every other edge from configuration, checked on both sides.

Plan decision 8 and design item 6. Environment names are free (`[a-z][a-z0-9-]*`). The
image compiles one rule: a sink marked `prod` accepts only a source marked `dev`. Every
other edge is declared twice: by the side that dials (the `edges` list of its config file)
and by the side that listens (`receive --accept-from`, `serve --serve-to`).
"""
from __future__ import annotations

import pytest

from sync_tables.config import load_edges
from sync_tables.envs import PULL, PUSH, DIRECT, EdgePolicy, check_listener, check_rule, parse_marker
from sync_tables.log import Refused, SyncError
from sync_tables.protocol import loopback_to
from sync_tables.pull import SourceServer, pull
from test_exchange import DEV, INCR, PROD, Identity, engine, make_pair, seed

STAGING = Identity("staging", "4444")
DECLARED = [
    {"from": "dev", "to": "local", "transports": ["pull", "direct"]},
    {"from": "dev", "to": "prod", "transports": ["push"]},
    {"from": "dev", "to": "staging", "transports": ["push"]},
]


# ---------------------------------------------------------------------------
# The compiled rule
# ---------------------------------------------------------------------------

def test_prod_accepts_dev():
    check_rule("dev", "prod")


@pytest.mark.parametrize("source", ["staging", "preprod", "local", "qa", "warehouse"])
def test_prod_accepts_nothing_but_dev(source):
    with pytest.raises(Refused) as e:
        check_rule(source, "prod")
    assert e.value.code == "prod_only_from_dev"


def test_an_edge_joins_two_environments():
    with pytest.raises(Refused) as e:
        check_rule("dev", "dev")
    assert e.value.code == "edge_not_allowed"


@pytest.mark.parametrize("name", ["", "Dev", "1dev", "dev env", "dev/x", "-qa", None])
def test_an_environment_name_is_one_lowercase_word(name):
    with pytest.raises(Refused) as e:
        check_rule(name, "local")
    assert e.value.code == "unknown_environment"


@pytest.mark.parametrize("a,b", [("staging", "preprod"), ("dev", "qa"), ("qa", "local")])
def test_the_rule_leaves_every_other_edge_to_configuration(a, b):
    check_rule(a, b)


# ---------------------------------------------------------------------------
# The dialling side: the `edges` list of its config
# ---------------------------------------------------------------------------

def test_a_declared_edge_has_its_transports():
    policy = EdgePolicy.from_config(DECLARED)
    assert policy.transports("dev", "local") == (PULL, DIRECT)
    assert policy.pick("dev", "prod", PUSH) == PUSH


def test_an_undeclared_edge_is_refused():
    policy = EdgePolicy.from_config(DECLARED)
    for a, b in [("dev", "preprod"), ("local", "dev"), ("prod", "dev"), ("staging", "local")]:
        with pytest.raises(Refused) as e:
            policy.transports(a, b)
        assert e.value.code == "edge_not_allowed"


def test_no_config_declares_no_edge():
    with pytest.raises(Refused) as e:
        EdgePolicy.from_config(None).transports("dev", "local")
    assert e.value.code == "edge_not_allowed"
    assert "edges" in e.value.message


def test_a_transport_the_edge_does_not_declare_is_refused():
    policy = EdgePolicy.from_config(DECLARED)
    with pytest.raises(Refused) as e:
        policy.pick("dev", "local", PUSH)
    assert e.value.code == "transport_mismatch"


def test_a_config_cannot_open_an_edge_into_prod_from_anything_but_dev():
    with pytest.raises(Refused) as e:
        EdgePolicy.from_config([{"from": "staging", "to": "prod", "transports": ["push"]}])
    assert e.value.code == "prod_only_from_dev"


@pytest.mark.parametrize("raw", [
    {"from": "dev", "to": "local"},                                    # not a list
    [{"from": "dev", "transports": ["pull"]}],                         # no to
    [{"from": "dev", "to": "local", "transports": []}],                # no transport
    [{"from": "dev", "to": "local", "transports": ["carrier-pigeon"]}],
    [{"from": "dev", "to": "local", "transports": "pull"}],            # not a list
    [{"from": "dev", "to": "local", "transports": ["pull"]},
     {"from": "dev", "to": "local", "transports": ["direct"]}],        # twice
    ["dev->local"],
])
def test_a_malformed_edges_list_is_refused_at_load(raw):
    with pytest.raises(SyncError):
        EdgePolicy.from_config(raw)


def test_edges_load_from_config_yaml(tmp_path):
    (tmp_path / "config.yaml").write_text(
        "edges:\n  - {from: dev, to: local, transports: [pull]}\n", encoding="utf-8"
    )
    assert load_edges(str(tmp_path / "config.yaml")).transports("dev", "local") == (PULL,)
    assert load_edges(str(tmp_path / "missing.yaml")).declared == {}


# ---------------------------------------------------------------------------
# The listening side: what a sink accepts from, what a serving source serves to
# ---------------------------------------------------------------------------

def test_a_listener_checks_its_own_list():
    check_listener("dev", "prod", accepted={"dev"}, end="from")
    with pytest.raises(Refused) as e:
        check_listener("staging", "local", accepted={"dev"}, end="from")
    assert e.value.code == "edge_not_allowed"


def test_a_listener_list_of_none_leaves_only_the_rule():
    check_listener("dev", "staging", accepted=None, end="from")
    with pytest.raises(Refused) as e:
        check_listener("staging", "prod", accepted=None, end="from")
    assert e.value.code == "prod_only_from_dev"


def test_a_sink_refuses_a_source_it_does_not_accept_from():
    src, tgt, source, sink = make_pair(tgt_ident=STAGING, edge=("dev", "staging"), accept_from={"other"})
    seed(src)
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "edge_not_allowed"


def test_a_sink_takes_a_source_it_accepts_from():
    src, tgt, source, sink = make_pair(tgt_ident=STAGING, edge=("dev", "staging"), accept_from={"dev"})
    seed(src)
    assert source.run([INCR], loopback_to(sink)).failed == []


def test_a_prod_sink_refuses_staging_even_when_both_sides_declare_it():
    src, tgt, source, sink = make_pair(
        src_ident=STAGING, tgt_ident=PROD, edge=("staging", "prod"), accept_from={"staging"}
    )
    seed(src)
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "prod_only_from_dev"


def _served(server):
    from test_pull import served  # noqa: PLC0415

    return served(server)


def test_a_serving_source_serves_only_its_targets():
    from test_pull import SPEC, local_sink  # noqa: PLC0415

    src = engine()
    seed(src, n=3)
    _, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: DEV, serve_to={"elsewhere"})
    with pytest.raises(Refused) as e:
        pull([SPEC], ("dev", "local"), _served(server), sink)
    assert e.value.code == "edge_not_allowed"


# ---------------------------------------------------------------------------
# Markers carry any valid name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("env", ["qa", "warehouse", "pre-prod2"])
def test_a_marker_names_any_environment(env):
    assert parse_marker(f"celine:env={env}", "target") == env


@pytest.mark.parametrize("comment", ["celine:env=-qa", "celine:env=QA", "celine:env=a b", "celine:env="])
def test_a_marker_with_an_invalid_name_is_malformed(comment):
    with pytest.raises(Refused) as e:
        parse_marker(comment, "target")
    assert e.value.code == "marker_malformed"


# ---------------------------------------------------------------------------
# The CLI: edges from config.yaml, before any connection
# ---------------------------------------------------------------------------

def _no_connection(monkeypatch):
    import sync_tables.db as db  # noqa: PLC0415

    def boom(*a, **k):
        raise AssertionError("a connection was opened")

    monkeypatch.setattr(db, "make_source_engine", boom)
    monkeypatch.setattr(db, "make_engine", boom)


def test_the_cli_refuses_an_edge_its_config_does_not_declare(tmp_path, monkeypatch, capsys):
    from sync_tables.__main__ import main  # noqa: PLC0415

    monkeypatch.chdir(tmp_path)
    _no_connection(monkeypatch)
    assert main(["--from", "dev", "--to", "local"]) == 2
    out = capsys.readouterr().out
    assert "edge_not_allowed" in out and "config.yaml" in out


def test_the_cli_takes_the_transport_from_the_declared_edge(tmp_path, monkeypatch, capsys):
    from sync_tables.__main__ import main  # noqa: PLC0415

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(
        "edges:\n  - {from: dev, to: local, transports: [pull, direct]}\n"
        "  - {from: dev, to: prod, transports: [push]}\n", encoding="utf-8"
    )
    _no_connection(monkeypatch)
    assert main(["--from", "dev", "--to", "local", "--sink-url", "wss://sink.example.org"]) == 2
    assert main(["--from", "dev", "--to", "prod"]) == 2
    assert capsys.readouterr().out.count("transport_mismatch") == 2


@pytest.mark.parametrize("cmd", ["receive", "serve"])
def test_a_listener_will_not_start_without_its_edge_list(cmd, monkeypatch, capsys):
    from sync_tables.__main__ import main  # noqa: PLC0415

    for var in ("SYNC_ACCEPT_FROM", "SYNC_SERVE_TO", "SYNC_SINK_DSN", "SYNC_SOURCE_DSN"):
        monkeypatch.delenv(var, raising=False)
    _no_connection(monkeypatch)
    assert main([cmd, "--allow-schema", "main", "--token-file", "/nonexistent"]) == 2
    flag = "--accept-from" if cmd == "receive" else "--serve-to"
    assert flag in capsys.readouterr().out


def test_a_prod_sink_will_not_start_accepting_anything_but_dev():
    from sync_tables.__main__ import _check_listener_edges  # noqa: PLC0415

    _check_listener_edges("prod", {"dev"}, end="from")
    with pytest.raises(Refused) as e:
        _check_listener_edges("prod", {"dev", "staging"}, end="from")
    assert e.value.code == "prod_only_from_dev"
    with pytest.raises(Refused) as e:
        _check_listener_edges("staging", {"prod"}, end="to")
    assert e.value.code == "prod_only_from_dev"
    _check_listener_edges("dev", {"local", "qa"}, end="to")


def test_the_cli_reads_its_edges_from_the_config_it_is_given(tmp_path, monkeypatch, capsys):
    """An image ships no config.yaml: a push mounts one and names it with --config."""
    from sync_tables.__main__ import main  # noqa: PLC0415

    monkeypatch.chdir(tmp_path)
    mounted = tmp_path / "etc" / "sync.yaml"
    mounted.parent.mkdir()
    mounted.write_text("edges:\n  - {from: dev, to: prod, transports: [push]}\n", encoding="utf-8")
    _no_connection(monkeypatch)
    assert main(["--from", "dev", "--to", "prod", "--config", str(mounted)]) == 2
    assert "transport_mismatch" in capsys.readouterr().out  # declared: only the URL is missing
    monkeypatch.setenv("SYNC_CONFIG", str(mounted))
    assert main(["--from", "dev", "--to", "prod"]) == 2
    assert "transport_mismatch" in capsys.readouterr().out
