"""Named tokens, the forwarded client certificate check, and the listener's TLS modes."""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import os
import ssl
import threading
from urllib.parse import quote

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from sync_tables import certs
from sync_tables.__main__ import _listener_material
from sync_tables.auth import TokenStore, UpstreamCertCheck
from sync_tables.log import Refused, SyncError
from sync_tables.sink import Sink
from sync_tables.wss import channel_factory, client_tls, make_server, server_tls

from test_wss import PROD, engine, free_port

T1, T2 = "a" * 40, "b" * 40


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------

def test_each_holder_is_matched_by_name(tmp_path):
    (tmp_path / "alice").write_text(T1 + "\n")
    (tmp_path / "bob").write_text(T2)
    store = TokenStore(str(tmp_path))
    assert store.names == ["alice", "bob"]
    assert store.match(f"Bearer {T1}") == "alice"
    assert store.match(f"Bearer {T2}") == "bob"
    assert store.match("Bearer " + "c" * 40) is None
    assert store.match(None) is None


def test_a_single_file_is_one_holder(tmp_path):
    (tmp_path / "token").write_text(T1)
    assert TokenStore(str(tmp_path / "token")).match(f"Bearer {T1}") == "default"


def test_a_mounted_secrets_dot_entries_are_ignored(tmp_path):
    (tmp_path / "..data").mkdir()
    (tmp_path / "..2026_10_17").mkdir()
    (tmp_path / "alice").write_text(T1)
    assert TokenStore(str(tmp_path)).names == ["alice"]


def test_removing_a_holder_revokes_it_without_a_restart(tmp_path):
    (tmp_path / "alice").write_text(T1)
    (tmp_path / "bob").write_text(T2)
    clock = Clock()
    store = TokenStore(str(tmp_path), clock=clock)
    os.remove(tmp_path / "bob")
    assert store.match(f"Bearer {T2}") == "bob"  # not re-read yet
    clock.t += 11
    assert store.match(f"Bearer {T2}") is None
    assert store.match(f"Bearer {T1}") == "alice"


def test_a_broken_reload_keeps_the_previous_set(tmp_path, capsys):
    (tmp_path / "alice").write_text(T1)
    clock = Clock()
    store = TokenStore(str(tmp_path), clock=clock)
    (tmp_path / "bob").write_text("short")
    clock.t += 11
    assert store.match(f"Bearer {T1}") == "alice"
    out = capsys.readouterr().out
    assert "reload failed" in out and "short" not in out


@pytest.mark.parametrize("tokens, why", [
    ({}, "no token"),
    ({"alice": "short"}, "fewer than"),
    ({"Alice": T1}, "plain lower-case"),
    ({"alice": T1, "bob": T1}, "share one token"),
])
def test_bad_token_sets_are_refused(tokens, why):
    with pytest.raises(SyncError, match=why):
        TokenStore(tokens=tokens)


# ---------------------------------------------------------------------------
# The client certificate an ingress forwards
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pkis(tmp_path_factory):
    push = str(tmp_path_factory.mktemp("push"))
    certs.make_ca(push, purpose="push")
    certs.make_server(push, ["localhost"])
    certs.make_client(push, "source")
    other = str(tmp_path_factory.mktemp("other"))
    certs.make_ca(other, purpose="pull")
    certs.make_client(other, "developer")
    return push, other


def _pem(path):
    with open(path, "rb") as f:
        return f.read().decode()


def nginx_header(pem: str) -> str:
    return quote(pem, safe="")


def traefik_header(pem: str) -> str:
    der = x509.load_pem_x509_certificate(pem.encode()).public_bytes(serialization.Encoding.DER)
    return quote(base64.b64encode(der).decode(), safe="")


@pytest.mark.parametrize("fmt", [nginx_header, traefik_header])
def test_a_client_certificate_from_the_ca_passes(pkis, fmt):
    push, _ = pkis
    check = UpstreamCertCheck("X-Cert", f"{push}/ca.crt")
    assert check.verify(fmt(_pem(f"{push}/client-source.crt")))


def test_a_traefik_chain_uses_the_first_certificate(pkis):
    push, _ = pkis
    check = UpstreamCertCheck("X-Cert", f"{push}/ca.crt")
    chain = traefik_header(_pem(f"{push}/client-source.crt")) + "," + traefik_header(_pem(f"{push}/ca.crt"))
    assert check.verify(chain)


@pytest.mark.parametrize("which", ["other-ca-client", "server-cert", "ca-itself", "garbage", "empty", "missing"])
def test_anything_but_a_client_certificate_from_the_ca_is_refused(pkis, which):
    push, other = pkis
    check = UpstreamCertCheck("X-Cert", f"{push}/ca.crt")
    value = {
        "other-ca-client": nginx_header(_pem(f"{other}/client-developer.crt")),
        "server-cert": nginx_header(_pem(f"{push}/server.crt")),  # issued by the CA, but for servers
        "ca-itself": nginx_header(_pem(f"{push}/ca.crt")),
        "garbage": "not-a-certificate",
        "empty": "",
        "missing": None,
    }[which]
    assert not check.verify(value)


def test_an_expired_client_certificate_is_refused(pkis):
    push, _ = pkis
    later = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=400)
    check = UpstreamCertCheck("X-Cert", f"{push}/ca.crt", clock=lambda: later)
    assert not check.verify(nginx_header(_pem(f"{push}/client-source.crt")))


# ---------------------------------------------------------------------------
# Listener modes
# ---------------------------------------------------------------------------

def listener_args(tmp_path, **kw):
    tok = tmp_path / "tokens"
    tok.mkdir(exist_ok=True)
    (tok / "alice").write_text(T1)
    base = dict(allow_schema=["main"], allow_schemas=None, token_file=str(tok), tls_cert=None, tls_key=None,
                client_ca=None, tls_terminated_upstream=False, require_upstream_client_cert=None)
    base.update(kw)
    return argparse.Namespace(**base)


def test_a_sink_behind_an_ingress_must_prove_the_client_certificate(tmp_path):
    with pytest.raises(SyncError, match="nothing proves the ingress checked"):
        _listener_material(listener_args(tmp_path, tls_terminated_upstream=True), mutual=True)


def test_a_sink_behind_an_ingress_with_the_check_starts(tmp_path, pkis):
    push, _ = pkis
    a = listener_args(tmp_path, tls_terminated_upstream=True, client_ca=f"{push}/ca.crt",
                      require_upstream_client_cert="X-Forwarded-Tls-Client-Cert")
    _, tokens, tls, upstream = _listener_material(a, mutual=True)
    assert tls is None and upstream is not None and tokens.names == ["alice"]


def test_a_sink_with_its_own_tls_needs_the_client_ca(tmp_path, pkis):
    push, _ = pkis
    a = listener_args(tmp_path, tls_cert=f"{push}/server.crt", tls_key=f"{push}/server.key")
    with pytest.raises(SyncError, match="--client-ca"):
        _listener_material(a, mutual=True)


def test_a_serving_source_may_run_on_server_tls_alone(tmp_path, pkis):
    push, _ = pkis
    a = listener_args(tmp_path, tls_cert=f"{push}/server.crt", tls_key=f"{push}/server.key")
    _, _, tls, _ = _listener_material(a, mutual=False)
    assert tls.verify_mode == ssl.CERT_NONE


def run_sink(tokens, tls=None, upstream=None):
    sink = Sink(engine(), ["main"], identify=lambda c: PROD, log_summaries=True)
    port = free_port()
    server = make_server(sink, "127.0.0.1", port, tokens, tls, upstream_cert=upstream)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port


def test_server_tls_without_a_client_certificate_accepts_a_named_token(pkis, capsys):
    push, _ = pkis
    server, port = run_sink(TokenStore(tokens={"alice": T1}), server_tls(f"{push}/server.crt", f"{push}/server.key", None))
    try:
        ch = channel_factory(f"wss://localhost:{port}", client_tls(f"{push}/ca.crt"), T1)()
        ch.close()
        with pytest.raises(Refused) as e:
            channel_factory(f"wss://localhost:{port}", client_tls(f"{push}/ca.crt"), T2)()
        assert e.value.code == "auth_failed"
    finally:
        server.shutdown()
    out = capsys.readouterr().out
    assert "token holder alice" in out and T1 not in out


def test_without_a_verified_forwarded_certificate_the_request_is_refused_loudly(pkis):
    """An ingress that stopped checking client certificates: every run fails, by name."""
    push, _ = pkis
    upstream = UpstreamCertCheck("X-Forwarded-Tls-Client-Cert", f"{push}/ca.crt")
    server, port = run_sink(TokenStore(tokens={"push": T1}), None, upstream)
    try:
        # Plain WS here stands for the ingress: the listener sees no handshake. A wss://
        # client cannot dial it, so drive the handshake directly.
        from websockets.exceptions import InvalidStatus
        from websockets.sync.client import connect

        url = f"ws://127.0.0.1:{port}/sync/v1"
        with pytest.raises(InvalidStatus) as e:
            connect(url, additional_headers={"Authorization": f"Bearer {T1}"})
        assert e.value.response.status_code == 403
        forged = {"Authorization": f"Bearer {T1}", "X-Forwarded-Tls-Client-Cert": "Zm9yZ2Vk"}
        with pytest.raises(InvalidStatus) as e:
            connect(url, additional_headers=forged)
        assert e.value.response.status_code == 403
        good = {"Authorization": f"Bearer {T1}",
                "X-Forwarded-Tls-Client-Cert": traefik_header(_pem(f"{push}/client-source.crt"))}
        with connect(url, additional_headers=good):
            pass
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# The client's token
# ---------------------------------------------------------------------------

def test_the_client_takes_its_token_from_sync_token(monkeypatch, tmp_path):
    from sync_tables.__main__ import _client_token

    monkeypatch.setenv("SYNC_TOKEN", T1 + "\n")
    assert _client_token(argparse.Namespace(token_file=None)) == T1
    (tmp_path / "t").write_text(T2)
    with pytest.raises(SyncError, match="not both"):
        _client_token(argparse.Namespace(token_file=str(tmp_path / "t")))
    monkeypatch.setenv("SYNC_TOKEN", "short")
    with pytest.raises(SyncError, match="fewer than 32"):
        _client_token(argparse.Namespace(token_file=None))
    monkeypatch.delenv("SYNC_TOKEN")
    assert _client_token(argparse.Namespace(token_file=str(tmp_path / "t"))) == T2
