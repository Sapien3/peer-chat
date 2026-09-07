"""Tests for peer_peers.py: identity-keyed enrollment and endpoint resolution."""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(ROOT))
import peer_peers as pe  # noqa: E402
import peer_platform as pp  # noqa: E402

ME = os.getpid()
T1 = "11111111-1111-4111-8111-111111111111"
T2 = "22222222-2222-4222-8222-222222222222"
linux_only = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="/proc semantics")


class MiniStore:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);")

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))

    def close(self):
        self.db.close()


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path_factory):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path_factory.mktemp("empty-home")))


@pytest.fixture
def unix_socket(tmp_path):
    path = tmp_path / "c.sock"
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(path))
    srv.listen(1)
    yield str(path)
    srv.close()


def legacy_config(sock, pid=ME, identity=None, name="Fable"):
    return {"thread": T1, "owner_pid": 4242, "owner_identity": "o", "socket": "/run/x.sock", "codex_home": "/h",
            "peer": {"pid": pid, "identity": identity or pp.process_identity(pid), "socket": sock, "name": name}}


def spawn_sleeper():
    child = subprocess.Popen([sys.executable, "-c", "import sys,time; sys.stdout.write('r\\n'); sys.stdout.flush(); time.sleep(30)"],
                             stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "r"
    return child


# --------------------------------------------------------------------------
# keys, records, normalisation
# --------------------------------------------------------------------------


def test_codex_key_depends_on_canonical_home_and_thread(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    k1 = pe.codex_key(home, T1)
    assert k1 == pe.codex_key(str(home) + "/", T1) == pe.codex_key(tmp_path / "home" / ".." / "home", T1)
    assert k1.startswith("codex:") and k1.endswith(":" + T1) and len(k1.split(":")[1]) == 16
    assert k1 != pe.codex_key(tmp_path / "other", T1) and k1 != pe.codex_key(home, T2)


def test_legacy_config_normalises_to_map_keyed_by_identity(unix_socket):
    config = legacy_config(unix_socket)
    peers = pe.all_peers(config)
    identity = pp.process_identity(ME)
    assert list(peers) == [identity], "key equals the legacy identity so messages.peer keys stay valid"
    rec = peers[identity]
    assert rec["kind"] == "claude" and rec["key"] == identity and rec["pid"] == ME
    assert rec["socket"] == unix_socket and rec["name"] == "Fable" and rec["enrolled_by"] == "legacy"
    assert "peers" not in config, "all_peers never writes"


def test_peers_map_is_authority_over_conflicting_legacy(unix_socket):
    config = legacy_config(unix_socket, name="LegacyName")
    other_identity = "boot:999:1"
    config["peers"] = {other_identity: {"kind": "claude", "pid": 999, "identity": other_identity, "socket": "/o.sock", "name": "Only"}}
    peers = pe.all_peers(config)
    assert list(peers) == [other_identity], "legacy peer is not merged when a peers map exists"
    assert peers[other_identity]["key"] == other_identity


def test_all_peers_drops_incomplete_entries_and_recomputes_keys(tmp_path):
    home = tmp_path / "h"
    home.mkdir()
    config = {"peers": {
        "wrong-key": {"kind": "codex", "thread": T1, "codex_home": str(home) + "/", "name": "astra", "state_root": str(tmp_path)},
        "junk": {"kind": "codex", "thread": T1},
        "also-junk": "string",
        "c": {"kind": "claude", "pid": ME, "identity": pp.process_identity(ME), "socket": "/s"},
    }}
    peers = pe.all_peers(config)
    assert set(peers) == {pe.codex_key(home, T1), pp.process_identity(ME)}
    assert peers[pe.codex_key(home, T1)]["codex_home"] == str(home.resolve())
    assert pe.all_peers({}) == {} and pe.all_peers(None) == {} and pe.all_peers({"peer": "x"}) == {}


def test_claude_record_pins_live_process(unix_socket):
    row = {"pid": ME, "name": "example-project-2f", "socket": unix_socket, "session": "s-1"}
    rec = pe.claude_record(row)
    assert rec["kind"] == "claude" and rec["key"] == pp.process_identity(ME) == rec["identity"]
    assert rec["socket"] == unix_socket and rec["session"] == "s-1" and rec["enrolled_by"] == "connect"
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    with pytest.raises(ValueError, match="not live"):
        pe.claude_record({"pid": dead.pid, "socket": unix_socket})
    for bad in ({"pid": ME}, {"socket": unix_socket}, {"pid": "x", "socket": unix_socket}, "row"):
        with pytest.raises(ValueError):
            pe.claude_record(bad)


def test_codex_record_from_registry_row(tmp_path):
    home = tmp_path / "home"; home.mkdir()
    row = {"kind": "codex", "thread": T1, "id": T1, "name": "astra-peer-chat", "codex_home": str(home), "owner_identity": "o:1"}
    rec = pe.codex_record(row, tmp_path / "state", name=None)
    assert rec == {**rec, "kind": "codex", "key": pe.codex_key(home, T1), "name": "astra-peer-chat", "thread": T1,
                   "codex_home": str(home.resolve()), "state_root": str((tmp_path / "state").resolve()), "owner_identity": "o:1",
                   "enrolled_by": "connect"}
    assert pe.codex_record(row, tmp_path, name="custom")["name"] == "custom"
    with pytest.raises(ValueError):
        pe.codex_record({"kind": "claude", "pid": 1}, tmp_path)
    with pytest.raises(ValueError):
        pe.codex_record({"kind": "codex", "thread": T1}, tmp_path)


# --------------------------------------------------------------------------
# enroll
# --------------------------------------------------------------------------


def test_enroll_merges_and_preserves_everything_else(tmp_path, unix_socket):
    store = MiniStore(tmp_path / "inbox.sqlite")
    try:
        store.put("config", legacy_config(unix_socket))
        store.put("delivery", "auto"); store.put("remaining", 7); store.put("budget_limit", 12); store.put("phase", "idle")
        home = tmp_path / "home"; home.mkdir()
        rec = pe.codex_record({"kind": "codex", "thread": T2, "codex_home": str(home), "name": "other"}, tmp_path / "state")
        config = pe.enroll(store, rec)
        peers = pe.all_peers(config)
        assert set(peers) == {pp.process_identity(ME), rec["key"]}
        assert config["peer"] == legacy_config(unix_socket)["peer"], "legacy mirror kept for old daemons"
        assert config["thread"] == T1 and config["owner_identity"] == "o", "unrelated config fields untouched"
        assert store.get("delivery") == "auto" and store.get("remaining") == 7 and store.get("budget_limit") == 12 and store.get("phase") == "idle"
        assert store.get("config") == config
        renamed = dict(rec, name="renamed")
        config = pe.enroll(store, renamed)
        assert pe.all_peers(config)[rec["key"]]["name"] == "renamed" and len(pe.all_peers(config)) == 2
        with pytest.raises(ValueError):
            pe.enroll(store, {"kind": "codex", "thread": T1})
    finally:
        store.close()


def test_enroll_into_empty_config_sets_mirror_from_claude(tmp_path, unix_socket):
    store = MiniStore(tmp_path / "inbox.sqlite")
    try:
        rec = pe.claude_record({"pid": ME, "socket": unix_socket, "name": "Fable"})
        config = pe.enroll(store, rec)
        assert config["peer"] == {"pid": ME, "identity": rec["identity"], "socket": unix_socket, "name": "Fable"}
        assert list(config["peers"]) == [rec["identity"]]
    finally:
        store.close()


# --------------------------------------------------------------------------
# resolve: claude
# --------------------------------------------------------------------------


def test_resolve_claude_live_dead_and_bad_socket(tmp_path, unix_socket):
    rec = pe.claude_record({"pid": ME, "socket": unix_socket})
    assert pe.resolve_peer(rec) == {"pid": ME, "identity": pp.process_identity(ME), "socket": unix_socket}
    with pytest.raises(ValueError, match="socket path does not exist"):
        pe.resolve_peer(dict(rec, socket=str(tmp_path / "missing.sock")))
    plain = tmp_path / "plain"; plain.write_text("x")
    with pytest.raises(ValueError, match="not a socket"):
        pe.resolve_peer(dict(rec, socket=str(plain)))
    sleeper = spawn_sleeper()
    stale = pe.claude_record({"pid": sleeper.pid, "socket": unix_socket})
    sleeper.kill(); sleeper.wait()
    with pytest.raises(ValueError, match="not live"):
        pe.resolve_peer(stale)
    with pytest.raises(ValueError, match="incomplete"):
        pe.resolve_peer({"kind": "claude"})


# --------------------------------------------------------------------------
# resolve: codex (counterpart bridge state + verified owner)
# --------------------------------------------------------------------------


def make_counterpart(state_root, thread, home, *, owner_pid, runtime_pid, socket_path, cfg_thread=None, cfg_home=None, runtime_socket=None):
    store = MiniStore(state_root / thread / "inbox.sqlite")
    try:
        store.put("config", {"thread": cfg_thread or thread, "codex_home": str(cfg_home or home), "socket": socket_path,
                             "owner_pid": owner_pid, "owner_identity": pp.process_identity(owner_pid),
                             "peer": {"pid": 1, "identity": "x", "socket": "/x", "name": "x"}})
        if runtime_pid is not None:
            store.put("runtime", {"pid": runtime_pid, "identity": pp.process_identity(runtime_pid),
                                  "socket": runtime_socket or socket_path})
        else:
            with store.db:
                store.db.execute("DELETE FROM meta WHERE key='runtime'")
    finally:
        store.close()


def write_registration(state_root, thread, home, owner_pid):
    reg = state_root / "registry"
    reg.mkdir(parents=True, exist_ok=True)
    (reg / f"{thread}.json").write_text(json.dumps({"version": 1, "kind": "codex", "thread": thread, "codex_home": str(home),
                                                    "owner_pid": owner_pid, "owner_identity": pp.process_identity(owner_pid), "name": "n"}))


@pytest.fixture
def codex_env(tmp_path, unix_socket):
    home = tmp_path / "home"; home.mkdir()
    state_root = tmp_path / "state"
    owner = spawn_sleeper()
    daemon = spawn_sleeper()
    make_counterpart(state_root, T1, home, owner_pid=owner.pid, runtime_pid=daemon.pid, socket_path=unix_socket)
    write_registration(state_root, T1, home, owner.pid)
    rec = pe.codex_record({"kind": "codex", "thread": T1, "codex_home": str(home), "name": "astra"}, state_root)
    yield {"home": home, "state_root": state_root, "owner": owner, "daemon": daemon, "rec": rec, "socket": unix_socket}
    for p in (owner, daemon):
        p.kill(); p.wait()


def test_resolve_codex_happy_path_and_daemon_restart(codex_env):
    env = codex_env
    ep = pe.resolve_peer(env["rec"])
    assert ep == {"pid": env["daemon"].pid, "identity": pp.process_identity(env["daemon"].pid), "socket": env["socket"]}
    # daemon restarts: new runtime pid/identity, same owner and socket; record and key unchanged
    env["daemon"].kill(); env["daemon"].wait()
    with pytest.raises(ValueError, match="listener is not live"):
        pe.resolve_peer(env["rec"])
    new_daemon = spawn_sleeper()
    try:
        make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=new_daemon.pid, socket_path=env["socket"])
        ep2 = pe.resolve_peer(env["rec"])
        assert ep2["pid"] == new_daemon.pid and ep2["pid"] != ep["pid"] and ep2["socket"] == env["socket"]
    finally:
        new_daemon.kill(); new_daemon.wait()


def test_resolve_codex_owner_resume_via_registration(codex_env):
    env = codex_env
    env["owner"].kill(); env["owner"].wait()
    with pytest.raises(ValueError, match="owner process is not live"):
        pe.resolve_peer(env["rec"])
    new_owner = spawn_sleeper()
    try:
        make_counterpart(env["state_root"], T1, env["home"], owner_pid=new_owner.pid, runtime_pid=env["daemon"].pid, socket_path=env["socket"])
        with pytest.raises(ValueError, match="live verified owner"):
            pe.resolve_peer(env["rec"]), "registration still names the dead owner"
        write_registration(env["state_root"], T1, env["home"], new_owner.pid)
        assert pe.resolve_peer(env["rec"])["pid"] == env["daemon"].pid
        assert env["rec"]["key"] == pe.codex_key(env["home"], T1), "key never changes across resume"
    finally:
        new_owner.kill(); new_owner.wait()


@linux_only
def test_resolve_codex_owner_verified_by_real_writer_lock_when_no_registration(codex_env):
    env = codex_env
    (env["state_root"] / "registry" / f"{T1}.json").unlink()
    with pytest.raises(ValueError, match="live verified owner"):
        pe.resolve_peer(env["rec"])
    lock_dir = env["home"] / "thread-writer-locks"; lock_dir.mkdir()
    code = ("import fcntl,sys,time\nopen('/proc/self/comm','w').write('codex')\n"
            f"fh=open({str(lock_dir / f'{T1}.lock')!r},'a'); fcntl.flock(fh, fcntl.LOCK_EX)\n"
            "sys.stdout.write('l\\n'); sys.stdout.flush(); time.sleep(30)\n")
    locker = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    try:
        assert locker.stdout.readline().strip() == "l"
        with pytest.raises(ValueError, match="live verified owner"):
            pe.resolve_peer(env["rec"]), "lock holder is not the config owner"
        make_counterpart(env["state_root"], T1, env["home"], owner_pid=locker.pid, runtime_pid=env["daemon"].pid, socket_path=env["socket"])
        assert pe.resolve_peer(env["rec"])["pid"] == env["daemon"].pid
    finally:
        locker.kill(); locker.wait()


def test_resolve_codex_rejections(codex_env, tmp_path, monkeypatch):
    env = codex_env
    rec = env["rec"]
    with pytest.raises(ValueError, match="state not found"):
        pe.resolve_peer(dict(rec, state_root=str(tmp_path / "nowhere")))
    make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=env["daemon"].pid,
                     socket_path=env["socket"], cfg_thread=T2)
    with pytest.raises(ValueError, match="different thread"):
        pe.resolve_peer(rec)
    other_home = tmp_path / "other-home"; other_home.mkdir()
    make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=env["daemon"].pid,
                     socket_path=env["socket"], cfg_home=other_home)
    with pytest.raises(ValueError, match="different Codex home"):
        pe.resolve_peer(rec)
    make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=env["daemon"].pid,
                     socket_path=env["socket"], runtime_socket="/elsewhere.sock")
    with pytest.raises(ValueError, match="socket path is inconsistent"):
        pe.resolve_peer(rec)
    make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=env["daemon"].pid,
                     socket_path=str(tmp_path / "gone.sock"))
    with pytest.raises(ValueError, match="does not exist"):
        pe.resolve_peer(rec)
    make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=None, socket_path=env["socket"])
    with pytest.raises(ValueError, match="not running"):
        pe.resolve_peer(rec)
    make_counterpart(env["state_root"], T1, env["home"], owner_pid=env["owner"].pid, runtime_pid=env["daemon"].pid, socket_path=env["socket"])
    real_stat = Path.stat

    def foreign(self, *a, **k):
        st = real_stat(self, *a, **k)
        if self.name == "inbox.sqlite":
            class S:  # same fields, foreign uid
                st_uid = os.getuid() + 1
                st_mode = st.st_mode
            return S()
        return st
    monkeypatch.setattr(Path, "stat", foreign)
    with pytest.raises(ValueError, match="another user"):
        pe.resolve_peer(rec)
    monkeypatch.undo()
    corrupt = MiniStore(env["state_root"] / T1 / "inbox.sqlite")
    with corrupt.db:
        corrupt.db.execute("UPDATE meta SET value='{not json' WHERE key='config'")
    corrupt.close()
    with pytest.raises(ValueError, match="corrupt or busy"):
        pe.resolve_peer(rec)


def test_expected_endpoints_reports_failures(codex_env, unix_socket):
    env = codex_env
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    config = {"peers": {
        env["rec"]["key"]: env["rec"],
        pp.process_identity(ME): {"kind": "claude", "pid": ME, "identity": pp.process_identity(ME), "socket": unix_socket, "name": "me"},
        "boot:0:0": {"kind": "claude", "pid": dead.pid, "identity": "boot:0:0", "socket": unix_socket, "name": "gone"},
    }}
    endpoints, failures = pe.expected_endpoints(config)
    assert set(endpoints) == {env["daemon"].pid, ME}
    assert endpoints[ME]["kind"] == "claude" and endpoints[env["daemon"].pid]["kind"] == "codex"
    assert endpoints[env["daemon"].pid]["endpoint"]["socket"] == unix_socket
    assert failures == [("boot:0:0", "claude peer: process is not live or was replaced")]


# --------------------------------------------------------------------------
# select
# --------------------------------------------------------------------------


def test_select_peer(tmp_path, unix_socket):
    home = tmp_path / "home"; home.mkdir()
    codex = pe.codex_record({"kind": "codex", "thread": T1, "codex_home": str(home), "name": "Astra"}, tmp_path)
    codex2 = pe.codex_record({"kind": "codex", "thread": T2, "codex_home": str(home), "name": "astra"}, tmp_path)
    claude = pe.claude_record({"pid": ME, "socket": unix_socket, "name": "Fable"})
    config = {"peers": {codex["key"]: codex, codex2["key"]: codex2, claude["key"]: claude}}
    assert pe.select_peer(config, codex["key"]) == pe.all_peers(config)[codex["key"]]
    assert pe.select_peer(config, "Astra")["thread"] == T1
    assert pe.select_peer(config, "astra")["thread"] == T2
    with pytest.raises(ValueError, match="ambiguous"):
        pe.select_peer(config, "ASTRA")
    assert pe.select_peer(config, T2[:8])["thread"] == T2
    assert pe.select_peer(config, "Fable")["kind"] == "claude"
    with pytest.raises(ValueError, match="No enrolled peer"):
        pe.select_peer(config, "nobody")
    with pytest.raises(ValueError, match="candidates: none"):
        pe.select_peer({}, "x")
    with pytest.raises(ValueError):
        pe.select_peer(config, "")
    with pytest.raises(ValueError):
        pe.select_peer(config, T1[:7])


# --------------------------------------------------------------------------
# validation hardening
# --------------------------------------------------------------------------


def test_unknown_kind_is_rejected_not_coerced(unix_socket):
    identity = pp.process_identity(ME)
    config = {"peers": {
        identity: {"kind": "mystery", "pid": ME, "identity": identity, "socket": unix_socket},
        "legacy": {"pid": ME, "identity": identity, "socket": unix_socket},  # kind absent: legacy claude shape
    }}
    peers = pe.all_peers(config)
    assert list(peers) == [identity] and peers[identity]["kind"] == "claude"
    assert pe.all_peers({"peers": {"x": {"kind": "codex ", "thread": T1, "codex_home": "/h", "state_root": "/s"}}}) == {}


@pytest.mark.parametrize("thread", ["../../etc", "11111111-1111-4111-8111-11111111111", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", "", None, "x" * 36])
def test_codex_thread_must_be_canonical_uuid(tmp_path, thread):
    with pytest.raises(ValueError):
        pe.codex_key(tmp_path, thread)
    with pytest.raises(ValueError):
        pe.codex_record({"kind": "codex", "thread": thread, "codex_home": str(tmp_path)}, tmp_path)
    assert pe.all_peers({"peers": {"k": {"kind": "codex", "thread": thread, "codex_home": str(tmp_path), "state_root": str(tmp_path)}}}) == {}


@pytest.mark.parametrize("root", ["", "   ", "relative/state", None, "../up"])
def test_state_root_must_be_absolute_and_non_empty(tmp_path, root):
    with pytest.raises(ValueError, match="state_root"):
        pe.codex_record({"kind": "codex", "thread": T1, "codex_home": str(tmp_path)}, root)
    assert pe.all_peers({"peers": {"k": {"kind": "codex", "thread": T1, "codex_home": str(tmp_path), "state_root": root}}}) == {}
    rec = pe.codex_record({"kind": "codex", "thread": T1, "codex_home": str(tmp_path)}, tmp_path)
    with pytest.raises(ValueError, match="state_root"):
        pe.resolve_peer(dict(rec, state_root=root) if root is not None else {k: v for k, v in rec.items() if k != "state_root"})


def test_codex_home_must_be_absolute(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        pe.codex_record({"kind": "codex", "thread": T1, "codex_home": "relative/home"}, tmp_path)
    assert pe.all_peers({"peers": {"k": {"kind": "codex", "thread": T1, "codex_home": "rel", "state_root": str(tmp_path)}}}) == {}


def test_resolve_codex_requires_live_owner_pid_matching_owner_identity(codex_env):
    env = codex_env
    store = MiniStore(env["state_root"] / T1 / "inbox.sqlite")
    try:
        config = store.get("config")
        dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
        store.put("config", dict(config, owner_pid=dead.pid))  # identity string still names the live owner
    finally:
        store.close()
    with pytest.raises(ValueError, match="owner process is not live"):
        pe.resolve_peer(env["rec"])


def test_resolve_codex_rejects_listener_of_other_uid(codex_env, monkeypatch):
    env = codex_env
    monkeypatch.setattr(pp, "process_uid", lambda pid: os.getuid() + 1 if pid == env["daemon"].pid else os.getuid())
    with pytest.raises(ValueError, match="another user"):
        pe.resolve_peer(env["rec"])


def test_expected_endpoints_rejects_two_records_on_same_pid(unix_socket, tmp_path):
    identity = pp.process_identity(ME)
    alias = {"kind": "claude", "pid": ME, "identity": identity, "socket": unix_socket, "name": "alias"}
    config = {"peers": {identity: alias}}
    endpoints, failures = pe.expected_endpoints(config)
    assert set(endpoints) == {ME} and failures == []
    # a second record for the same process cannot exist under identity keys for claude,
    # so simulate the collision with a codex record whose resolution lands on our pid
    monkeypatch_target = pe.resolve_peer

    def fake_resolve(record, *, state_root=None):
        return {"pid": ME, "identity": identity, "socket": unix_socket}
    pe.resolve_peer = fake_resolve
    try:
        home = tmp_path / "h"; home.mkdir()
        codex = pe.codex_record({"kind": "codex", "thread": T1, "codex_home": str(home)}, tmp_path)
        config["peers"][codex["key"]] = codex
        with pytest.raises(ValueError, match="same process pid"):
            pe.expected_endpoints(config)
    finally:
        pe.resolve_peer = monkeypatch_target
