"""Tests for peer_observe.py: read-only bridge snapshots and human rendering."""

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(ROOT))
import peer_observe as po  # noqa: E402
import peer_platform as pp  # noqa: E402

ME = os.getpid()
T1 = "11111111-1111-4111-8111-111111111111"
T2 = "22222222-2222-4222-8222-222222222222"
T3 = "33333333-3333-4333-8333-333333333333"
SECRET = "SECRET-BODY-NEVER-SHOWN"
SEEN = {"at": time.time() - 30, "event": "PostToolUse", "turn": "t-1", "routing": "matched"}


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path_factory):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path_factory.mktemp("empty-home")))


def spawn_sleeper():
    child = subprocess.Popen([sys.executable, "-c", "import sys,time; sys.stdout.write('r\\n'); sys.stdout.flush(); time.sleep(30)"],
                             stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "r"
    return child


SOCKETS = []


def bind_socket(path):
    import socket as _socket
    srv = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    srv.bind(str(path))
    srv.listen(1)
    SOCKETS.append(srv)
    return str(path)


@pytest.fixture(autouse=True)
def _close_sockets():
    yield
    while SOCKETS:
        SOCKETS.pop().close()


def make_bridge(state_root, thread, *, owner_pid=ME, runtime_pid=ME, delivery="auto", remaining=5, wake_remaining=None,
                budget_limit=12, phase="active", hook_seen=SEEN, stop=None, messages=(), outgoing=(), extra_meta=None, peers=None,
                socket_path="auto", supervision=True):
    d = Path(state_root) / thread
    d.mkdir(parents=True, exist_ok=True)
    if socket_path == "auto":
        socket_path = bind_socket(d / "l.sock") if runtime_pid is not None and not (d / "l.sock").exists() else str(d / "l.sock")
    conn = sqlite3.connect(d / "inbox.sqlite")
    conn.executescript("""
      CREATE TABLE IF NOT EXISTS messages (peer TEXT, id TEXT, kind TEXT, body TEXT, sender_mode TEXT, hops TEXT,
        status TEXT, detail TEXT DEFAULT '', created REAL, PRIMARY KEY(peer,id,kind));
      CREATE TABLE IF NOT EXISTS outgoing (id TEXT PRIMARY KEY, body TEXT, status TEXT, detail TEXT DEFAULT '', created REAL, peer TEXT);
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
    """)
    config = {"thread": thread, "socket": socket_path, "codex_home": "/h", "supervision": supervision,
              "owner_pid": owner_pid, "owner_identity": pp.process_identity(owner_pid) or "gone",
              "supervision": supervision,
              "peer": {"pid": 4242, "identity": "boot:4242:1", "socket": "/c.sock", "name": "Fable"}}
    if peers is not None:
        config["peers"] = peers
    meta = {"config": config, "delivery": delivery, "remaining": remaining, "budget_limit": budget_limit,
            "phase": phase, "hook_seen": hook_seen}
    if wake_remaining is not None:
        meta["wake_remaining"] = wake_remaining
    if runtime_pid is not None:
        meta["runtime"] = {"pid": runtime_pid, "identity": pp.process_identity(runtime_pid) or "gone", "socket": config["socket"]}
    if stop is not None:
        meta["stop"] = stop
    meta.update(extra_meta or {})
    for k, v in meta.items():
        conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, json.dumps(v)))
    for i, status in enumerate(messages):
        conn.execute("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?)",
                     ("boot:4242:1", f"m-{i}", "message", SECRET, "bypass", "[]", status, "", time.time()))
    for i, status in enumerate(outgoing):
        conn.execute("INSERT INTO outgoing VALUES (?,?,?,?,?,?)", (f"o-{i}", SECRET, status, "", time.time(), None))
    conn.commit()
    conn.close()
    return d / "inbox.sqlite"


# --------------------------------------------------------------------------
# snapshot
# --------------------------------------------------------------------------


def test_snapshot_running_reachable_bridge(tmp_path):
    make_bridge(tmp_path, T1, messages=("consumed", "consumed", "received", "hook_offered"), outgoing=("written",))
    (tmp_path / "registry").mkdir()
    (tmp_path / "registry" / f"{T1}.json").write_text(json.dumps({"name": "astra-peer-chat", "thread": T1}))
    row = po.snapshot(tmp_path, T1)
    assert row["name"] == "astra-peer-chat" and row["thread"] == T1
    assert row["runtime_state"] == "running" and row["delivery_state"] == "active_hooks" and row["reachable"] is True
    assert row["pending_count"] == 2 and row["pending_by_status"] == {"received": 1, "hook_offered": 1}
    assert row["remaining"] == 5 and row["budget_limit"] == 12 and row["wake_remaining"] is None
    assert row["phase"] == "active" and row["hook_seen"]["event"] == "PostToolUse" and 0 <= row["hook_age_s"] < 120
    assert row["runtime_pid"] == ME and row["owner_pid"] == ME
    assert row["peers"] == [{"kind": "claude", "name": "Fable", "key": "boot:4242:1", "key_short": "4242:1"}]
    assert row["warning"] is None and row["error"] is None and row["outgoing_uncertain"] == 0
    assert SECRET not in json.dumps(row), "bodies never enter a snapshot"


def test_snapshot_missing_or_invalid(tmp_path):
    assert po.snapshot(tmp_path, T1) is None
    assert po.snapshot(tmp_path, "not-a-uuid") is None
    (tmp_path / T2).mkdir()
    assert po.snapshot(tmp_path, T2) is None, "directory without a database is not a bridge"


def test_snapshot_listener_down_and_stopped_and_owner_offline(tmp_path):
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    make_bridge(tmp_path, T1, runtime_pid=dead.pid, messages=("received",))
    row = po.snapshot(tmp_path, T1)
    assert row["runtime_state"] == "listener_down" and row["delivery_state"] == "listener_down"
    assert row["delivery_configured"] == "active_hooks", "what it would be if the listener were up"
    assert row["reachable"] is False and row["pending_count"] == 1
    assert "restart" in row["warning"] and T1 in row["warning"]
    make_bridge(tmp_path, T2, runtime_pid=None, stop=True)
    row = po.snapshot(tmp_path, T2)
    assert row["runtime_state"] == "stopped" and "stopped explicitly" in row["warning"]
    make_bridge(tmp_path, T3, owner_pid=dead.pid, runtime_pid=dead.pid)
    row = po.snapshot(tmp_path, T3)
    assert row["runtime_state"] == "owner_offline" and "Resume the thread" in row["warning"] and row["listener_alive"] is False


def test_owner_offline_beats_running_listener_and_stop_beats_all(tmp_path):
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    make_bridge(tmp_path, T1, owner_pid=dead.pid, runtime_pid=ME, messages=("received",))
    row = po.snapshot(tmp_path, T1)
    assert row["runtime_state"] == "owner_offline" and row["listener_alive"] is True and row["socket_ok"] is True
    assert row["delivery_state"] == "owner_offline" and row["reachable"] is False
    assert "listener is retaining messages" in row["warning"] and "watchdog reconnects" in row["warning"]
    make_bridge(tmp_path, T2, runtime_pid=ME, stop=True)
    row = po.snapshot(tmp_path, T2)
    assert row["runtime_state"] == "stopped" and row["listener_alive"] is True
    assert "stopped explicitly" in row["warning"] and "watchdog respects the stop" in row["warning"]


def test_live_pid_with_dead_socket_is_listener_down(tmp_path):
    make_bridge(tmp_path, T1, socket_path=str(tmp_path / "missing.sock"))
    row = po.snapshot(tmp_path, T1)
    assert row["runtime_state"] == "listener_down" and row["listener_alive"] is True and row["socket_ok"] is False
    assert "socket is missing or not ours" in row["warning"]
    plain = tmp_path / "plain.sock"; plain.write_text("x")
    make_bridge(tmp_path, T2, socket_path=str(plain))
    assert po.snapshot(tmp_path, T2)["runtime_state"] == "listener_down"
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    make_bridge(tmp_path, T3, runtime_pid=dead.pid)
    row = po.snapshot(tmp_path, T3)
    assert row["runtime_state"] == "listener_down" and row["listener_alive"] is False
    assert "listener process is not running" in row["warning"] and "watchdog restarts it" in row["warning"]


def test_bridge_dir_symlink_and_non_regular_db_rejected(tmp_path):
    make_bridge(tmp_path, T1)
    (tmp_path / T2).symlink_to(tmp_path / T1)
    row = po.snapshot(tmp_path, T2)
    assert row["runtime_state"] == "unreadable" and row["error"] == "bridge directory is a symlink"
    d = tmp_path / T3; d.mkdir()
    os.mkfifo(d / "inbox.sqlite")
    row = po.snapshot(tmp_path, T3)
    assert row["runtime_state"] == "unreadable" and row["error"] == "state database is not a regular file"
    assert [r["thread"] for r in po.snapshots(tmp_path)] == [T1, T3], "symlinked dirs skipped entirely"


@pytest.mark.parametrize("delivery,remaining,phase,seen,expected,fragment", [
    ("inbox", 5, "active", SEEN, "manual_inbox", "read --ack"),
    ("auto", 0, "active", SEEN, "paused_budget", "budget exhausted"),
    ("queue", 5, "active", SEEN, "after_turn_queue", "after the current turn"),
    ("auto", 5, "active", None, "awaiting_lifecycle_hook", "peer-chat-setup once"),
    ("auto", 5, "unknown", SEEN, "awaiting_lifecycle_hook", "resume the thread"),
    ("live", 5, "idle", SEEN, "idle_live_only", "cannot wake an idle model"),
    ("live", 5, "active", SEEN, "active_hooks", None),
    ("auto", 5, "idle", SEEN, "idle_wake_enabled", None),
])
def test_snapshot_delivery_states_and_warnings(tmp_path, delivery, remaining, phase, seen, expected, fragment):
    make_bridge(tmp_path, T1, delivery=delivery, remaining=remaining, phase=phase, hook_seen=seen)
    row = po.snapshot(tmp_path, T1)
    assert row["delivery_state"] == expected
    if fragment is None:
        assert row["warning"] is None and row["reachable"] is True
    else:
        assert fragment in row["warning"] and row["reachable"] is False


def test_snapshot_budget_warning_mentions_limit_or_configure(tmp_path):
    make_bridge(tmp_path, T1, remaining=0, budget_limit=12)
    assert "limit 12" in po.snapshot(tmp_path, T1)["warning"]
    make_bridge(tmp_path, T2, remaining=0, budget_limit=None)
    assert "--budget" in po.snapshot(tmp_path, T2)["warning"]
    make_bridge(tmp_path, T3, remaining=3, wake_remaining=0, phase="idle")
    row = po.snapshot(tmp_path, T3)
    assert row["delivery_state"] == "paused_wake_budget" and "Wake budget" in row["warning"] and row["wake_remaining"] == 0


def test_snapshot_worker_restore_and_outgoing_uncertain(tmp_path):
    make_bridge(tmp_path, T1, extra_meta={"worker_error": "OperationalError"})
    assert "Dispatcher stopped (OperationalError)" in po.snapshot(tmp_path, T1)["warning"]
    make_bridge(tmp_path, T2, extra_meta={"restore_error": "Previous peer exited"})
    assert "restore failed: Previous peer exited" in po.snapshot(tmp_path, T2)["warning"]
    make_bridge(tmp_path, T3, outgoing=("written", "uncertain", "pending"))
    row = po.snapshot(tmp_path, T3)
    assert row["outgoing_uncertain"] == 2 and "2 outgoing write(s) uncertain" in row["warning"]


def test_snapshot_unconfigured_corrupt_symlink_and_foreign_owner(tmp_path, monkeypatch):
    d = tmp_path / T1; d.mkdir()
    conn = sqlite3.connect(d / "inbox.sqlite")
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit(); conn.close()
    row = po.snapshot(tmp_path, T1)
    assert row["runtime_state"] == "unconfigured" and "no configuration" in row["warning"]
    assert "/hooks" not in json.dumps(po.snapshots(tmp_path)), "no manual /hooks instructions anywhere"
    d2 = tmp_path / T2; d2.mkdir()
    (d2 / "inbox.sqlite").write_bytes(b"this is not sqlite at all" * 100)
    row = po.snapshot(tmp_path, T2)
    assert row["runtime_state"] == "unreadable" and row["error"] and "doctor" in row["warning"]
    real = make_bridge(tmp_path, T3)
    d3 = tmp_path / "44444444-4444-4444-8444-444444444444"; d3.mkdir()
    (d3 / "inbox.sqlite").symlink_to(real)
    row = po.snapshot(tmp_path, "44444444-4444-4444-8444-444444444444")
    assert row["runtime_state"] == "unreadable" and row["error"] == "state database is a symlink"
    real_lstat = Path.lstat

    def foreign(self):
        st = real_lstat(self)
        if self.name == "inbox.sqlite" and self.parent.name == T3:
            class S:
                st_uid = os.getuid() + 1
                st_mode = st.st_mode
            return S()
        return st
    monkeypatch.setattr(Path, "lstat", foreign)
    row = po.snapshot(tmp_path, T3)
    assert row["error"] == "state database belongs to another user" and row["runtime_state"] == "unreadable"


def test_snapshot_never_writes(tmp_path):
    path = make_bridge(tmp_path, T1, messages=("received",))
    before = path.read_bytes()
    mtime = path.stat().st_mtime_ns
    po.snapshot(tmp_path, T1); po.snapshots(tmp_path)
    assert path.read_bytes() == before and path.stat().st_mtime_ns == mtime
    assert not (tmp_path / T1 / "inbox.sqlite-wal").exists() or (tmp_path / T1 / "inbox.sqlite-wal").stat().st_size == 0


def test_snapshot_tolerates_corrupt_meta_values(tmp_path):
    make_bridge(tmp_path, T1)
    conn = sqlite3.connect(tmp_path / T1 / "inbox.sqlite")
    conn.execute("UPDATE meta SET value='{not json' WHERE key='phase'")
    conn.execute("UPDATE meta SET value='\"active\"' WHERE key='remaining'")
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('hook_seen', '\"string\"')")
    conn.commit(); conn.close()
    row = po.snapshot(tmp_path, T1)
    assert row["phase"] == "unknown" and row["remaining"] is None and row["hook_seen"] is None
    assert row["delivery_state"] in ("paused_budget", "awaiting_lifecycle_hook", "unknown")


# --------------------------------------------------------------------------
# snapshots
# --------------------------------------------------------------------------


def test_snapshots_lists_all_including_dead_and_sorts_running_first(tmp_path):
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    make_bridge(tmp_path, T2, runtime_pid=dead.pid)
    make_bridge(tmp_path, T1)
    (tmp_path / "registry").mkdir()
    (tmp_path / "44444444-4444-4444-8444-444444444444").mkdir()  # no db
    (tmp_path / "notes.txt").write_text("x")
    (tmp_path / "archive").mkdir()
    d = tmp_path / T3; d.mkdir()
    (d / "inbox.sqlite").write_bytes(b"garbage")
    rows = po.snapshots(tmp_path)
    assert [r["thread"] for r in rows] == [T1, T2, T3]
    assert [r["runtime_state"] for r in rows] == ["running", "listener_down", "unreadable"]
    assert po.snapshots(tmp_path / "missing") == []


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------


def test_sanitize_strips_control_and_escapes():
    assert po.sanitize("ok\x1b[31mred\x1b[0m\n\ttab\x00nul") == "ok?red?  tab?nul"
    text = po.sanitize("a\x1b[2Jb\r\nc\x00d\x7fe\x1b]0;title\x07f")
    assert text == "a?b  c?d?e?f"
    assert "\x1b" not in text and "\n" not in text and "\x00" not in text and "\x7f" not in text
    assert po.sanitize("x" * 50, 10) == "x" * 9 + "…"
    assert po.sanitize(None) == "" and po.sanitize(12) == "12"
    assert po.sanitize("‮evil") == "?evil"


def test_render_status_table_and_warnings(tmp_path):
    make_bridge(tmp_path, T1, messages=("received", "consumed"))
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    make_bridge(tmp_path, T2, runtime_pid=dead.pid, remaining=0)
    (tmp_path / "registry").mkdir()
    (tmp_path / "registry" / f"{T2}.json").write_text(json.dumps({"name": "evil\x1b[31mname\nline2", "thread": T2}))
    rows = po.snapshots(tmp_path)
    text = po.render_status(rows)
    lines = text.splitlines()
    assert lines[0].split() == ["NAME", "THREAD", "RUNTIME", "DELIVERY", "PENDING", "OLDEST", "BUDGET", "PHASE", "HOOK"]
    assert T1[:8] in lines[1] and "running" in lines[1] and "active_hooks" in lines[1] and " 5/12 " in lines[1]
    assert "PostToolUse" in lines[1]
    assert "listener_down" in lines[2] and "0/12" in lines[2]
    assert "\x1b" not in text and "line2" in text, "sanitized name"
    assert sum(1 for line in text.splitlines() if line.startswith("! ")) == 1, "one warning line"
    assert lines[3].startswith("! evil?name line2 (22222222): Listener down")
    assert SECRET not in text
    assert po.render_status([]) == "No bridges saved."


def test_render_sessions_table_from_registry_rows_and_snapshots(tmp_path):
    reg_rows = [
        {"kind": "codex", "thread": T1, "id": T1, "name": "astra", "owner_pid": 100, "pid": 100, "source": "hook:Stop", "phase": "idle",
         "initialization_required": False, "bridge": {"runtime_alive": True}, "delivery_state": "idle_wake_enabled"},
        {"kind": "codex", "thread": T2, "id": T2, "name": "blank\x1b[0mtab", "pid": 200, "source": "writer-lock", "phase": "unknown",
         "initialization_required": True, "bridge": None},
        {"kind": "claude", "id": "s-1", "name": "Fable", "pid": 300, "cwd": "/w"},
    ]
    text = po.render_sessions(reg_rows)
    lines = text.splitlines()
    assert lines[0].split() == ["NAME", "THREAD", "PID", "KIND/SOURCE", "PHASE", "DELIVERY"]
    assert "astra" in lines[1] and "codex/hook:Stop" in lines[1] and lines[1].rstrip().endswith("idle_wake_enabled")
    assert "blank?tab" in lines[2] and "needs first pr" in lines[2] and lines[2].rstrip().endswith("not_connected")
    assert "Fable" in lines[3] and "claude/-" in lines[3]
    make_bridge(tmp_path, T3)
    snap_text = po.render_sessions(po.snapshots(tmp_path))
    assert T3[:8] in snap_text and snap_text.rstrip().endswith("active_hooks")
    assert po.render_sessions([]) == "No sessions found."


def test_helper_is_read_only_by_construction():
    src = (ROOT / "peer_observe.py").read_text()
    for forbidden in ("INSERT", "UPDATE", "DELETE", "CREATE TABLE", "subprocess", "socket.socket", "Store("):
        assert forbidden not in src, forbidden
    assert "mode=ro" in src


# --------------------------------------------------------------------------
# live-use anomalies: full keys, effective wake allowance, orphans, names, stale evidence
# --------------------------------------------------------------------------


def test_keys_are_full_in_data_and_distinguishing_when_short(tmp_path):
    home_hash = "0d413f9782b66930"
    codex_a = f"codex:{home_hash}:{T1}"
    codex_b = f"codex:{home_hash}:{T2}"
    claude_a = "f8755910-7e73-4f0b-9714-9d5c1025b96e:1554184:17524518"
    claude_b = "f8755910-7e73-4f0b-9714-9d5c1025b96e:1649166:18194152"
    peers = {codex_a: {"kind": "codex", "thread": T1, "codex_home": "/h", "state_root": "/s", "name": "a"},
             codex_b: {"kind": "codex", "thread": T2, "codex_home": "/h", "state_root": "/s", "name": "b"},
             claude_a: {"kind": "claude", "pid": 1554184, "identity": claude_a, "socket": "/x", "name": "ca"},
             claude_b: {"kind": "claude", "pid": 1649166, "identity": claude_b, "socket": "/y", "name": "cb"}}
    make_bridge(tmp_path, T1, peers=peers)
    conn = sqlite3.connect(tmp_path / T1 / "inbox.sqlite")
    cfg = json.loads(conn.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
    cfg["default_peer"] = claude_b
    conn.execute("UPDATE meta SET value=? WHERE key='config'", (json.dumps(cfg),)); conn.commit(); conn.close()
    row = po.snapshot(tmp_path, T1)
    keys = [p["key"] for p in row["peers"]]
    assert len(set(keys)) == 4 and all(len(k) > 20 for k in keys), "raw keys are full, never truncated"
    shorts = [p["key_short"] for p in row["peers"]]
    assert len(set(shorts)) == 4, "display forms stay distinguishable"
    assert po.key_short(codex_a) == "codex:11111111" and po.key_short(claude_b) == "1649166:18194152"
    assert row["default_peer"] == claude_b and row["default_peer_short"] == "1649166:18194152"
    assert po.key_short(None) == "" and po.key_short("plain") == "plain"


def test_effective_wake_allowance_reported_without_mutation(tmp_path):
    path = make_bridge(tmp_path, T1, remaining=12, wake_remaining=None, budget_limit=None)
    before = path.read_bytes()
    row = po.snapshot(tmp_path, T1)
    assert row["wake_remaining"] is None and row["wake_remaining_effective"] == 12 and row["wake_remaining_source"] == "legacy_fallback"
    assert path.read_bytes() == before, "the legacy window is never written"
    make_bridge(tmp_path, T2, remaining=7, wake_remaining=3)
    row = po.snapshot(tmp_path, T2)
    assert row["wake_remaining_effective"] == 3 and row["wake_remaining_source"] == "explicit"
    text = po.render_status(po.snapshots(tmp_path))
    assert " w12~" in text and " w3 " in text and "~ = wake allowance follows" in text
    make_bridge(tmp_path, T3, remaining=None)
    row = po.snapshot(tmp_path, T3)
    assert row["wake_remaining_effective"] is None and row["wake_remaining_source"] is None


def test_orphan_state_dirs_hidden_by_default_and_counted(tmp_path):
    make_bridge(tmp_path, T1)
    d = tmp_path / T2; d.mkdir()
    conn = sqlite3.connect(d / "inbox.sqlite")
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)"); conn.commit(); conn.close()
    rows = po.snapshots(tmp_path)
    assert [r["orphan"] for r in rows] == [False, True]
    text = po.render_status(rows)
    body_lines = text.splitlines()[1:2]
    assert all(T2[:8] not in line for line in body_lines) and "1 orphan state directory hidden" in text and "--all" in text
    assert T2[:8] in text.splitlines()[-1]
    full = po.render_status(rows, show_orphans=True)
    assert "unconfigured" in full and "orphan state" not in full
    d3 = tmp_path / T3; d3.mkdir()
    conn = sqlite3.connect(d3 / "inbox.sqlite")
    conn.executescript("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT); CREATE TABLE messages (peer TEXT, id TEXT, kind TEXT, body TEXT, sender_mode TEXT, hops TEXT, status TEXT, detail TEXT, created REAL);")
    conn.execute("INSERT INTO messages VALUES ('p','m','message','x','bypass','[]','received','',1)"); conn.commit(); conn.close()
    row = po.snapshot(tmp_path, T3)
    assert row["runtime_state"] == "unconfigured" and row["orphan"] is False and row["pending_count"] == 1


def test_long_names_keep_their_distinguishing_tail(tmp_path):
    (tmp_path / "registry").mkdir()
    make_bridge(tmp_path, T1); make_bridge(tmp_path, T2)
    for t in (T1, T2):
        (tmp_path / "registry" / f"{t}.json").write_text(json.dumps({"name": f"discovery-scale-week1-extra-{t[:8]}", "thread": t}))
    text = po.render_status(po.snapshots(tmp_path))
    lines = text.splitlines()
    assert lines[1].split()[0] != lines[2].split()[0], "truncated names must still differ"
    assert lines[1].split()[0].endswith(T1[:8]) and lines[2].split()[0].endswith(T2[:8])
    assert po.sanitize("discovery-scale-week1-11111111", 20, 9) == "discovery-…-11111111"
    assert po.sanitize("short", 20, 9) == "short"
    assert po.sanitize("x" * 30, 5, 9) == "xxxx…", "tail keeping is skipped when the width cannot hold it"


def test_stale_evidence_marked_when_listener_not_running(tmp_path):
    make_bridge(tmp_path, T1, runtime_pid=None, stop=True, phase="active")
    make_bridge(tmp_path, T2, runtime_pid=ME, phase="active")
    rows = {r["thread"]: r for r in po.snapshots(tmp_path)}
    assert rows[T1]["evidence_stale"] is True and rows[T2]["evidence_stale"] is False
    text = po.render_status(list(rows.values()))
    assert "active?" in text and "? = last known phase" in text


def test_healthy_but_unsupervised_bridge_warns_and_stays_reachable(tmp_path):
    make_bridge(tmp_path, T1, supervision=False)
    row = po.snapshot(tmp_path, T1)
    assert row["supervision_enabled"] is False and row["reachable"] is True and row["delivery_state"] == "active_hooks"
    assert "Automatic recovery is not enabled" in row["warning"] and "watch start" in row["warning"]
    make_bridge(tmp_path, T2, supervision=True)
    row = po.snapshot(tmp_path, T2)
    assert row["supervision_enabled"] is True and row["warning"] is None
    # a real problem still outranks the supervision note
    make_bridge(tmp_path, T3, supervision=False, remaining=0)
    assert "budget exhausted" in po.snapshot(tmp_path, T3)["warning"]


def test_unsupervised_live_bridge_is_visible_without_hiding_write_failure(tmp_path):
    make_bridge(tmp_path, T1, supervision=False)
    row = po.snapshot(tmp_path, T1)
    assert row['supervision_enabled'] is False and row['reachable'] is True
    assert 'recovery is not enabled' in row['warning']
    make_bridge(tmp_path, T2, supervision=False, outgoing=('uncertain',))
    assert 'outgoing write(s) uncertain' in po.snapshot(tmp_path, T2)['warning']


def test_hook_warning_is_not_truncated_before_the_action(tmp_path):
    make_bridge(tmp_path, T1, hook_seen=None)
    row = po.snapshot(tmp_path, T1)
    rendered = po.render_status([row])
    assert 'awaiting_lifecycle_hook' in rendered
    assert row['warning'] in rendered


def test_status_cli_all_alias_and_unknown_thread_do_not_create_state(tmp_path):
    make_bridge(tmp_path, T1)
    orphan = tmp_path/T2;orphan.mkdir()
    with sqlite3.connect(orphan/'inbox.sqlite') as db:
        db.execute('CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT)')
    base = [sys.executable, str(ROOT/'peer_chat.py'), '--state-root', str(tmp_path), 'status']
    def run(*args):
        return subprocess.run(base+list(args), capture_output=True, text=True, timeout=5)
    normal = run();assert normal.returncode == 0
    assert 'orphan state directory hidden' in normal.stdout
    assert 'unconfigured' in run('--all').stdout
    one = run('--thread', T1)
    assert one.returncode == 0 and json.loads(one.stdout)['thread'] == T1
    unknown = run('--thread', T3)
    assert unknown.returncode == 1 and 'No saved bridge' in unknown.stderr
    assert not (tmp_path/T3).exists()


def test_advisory_failure_note_reads_as_a_separate_sentence(tmp_path):
    failures = {"notice:peer-a": {"at": 1.0, "result": "unconfirmed:ValueError"},
                "notice:peer-b": {"at": 2.0, "result": "unconfirmed:OSError"},
                "notice:peer-c": {"at": 3.0, "result": "written"}}
    make_bridge(tmp_path, T1, extra_meta=failures)               # healthy: note stands alone
    row = po.snapshot(tmp_path, T1)
    assert len(row["notice_failures"]) == 2 and {f["peer"] for f in row["notice_failures"]} == {"peer-a", "peer-b"}
    assert row["warning"] == ("2 peer advisory attempt(s) unconfirmed; no automatic resend. "
                              "Inspect notice_failures in status --json.")
    make_bridge(tmp_path, T2, hook_seen=None, extra_meta=failures)  # a real warning precedes it
    row = po.snapshot(tmp_path, T2)
    head, _, tail = row["warning"].partition("2 peer advisory attempt(s)")
    assert head.endswith(". "), "the preceding warning must end with a full stop and a space"
    assert "automatically 2 peer" not in row["warning"] and tail
    make_bridge(tmp_path, T3, remaining=0, extra_meta=failures)     # warning already ends in a full stop
    row = po.snapshot(tmp_path, T3)
    assert ".. " not in row["warning"] and ". 2 peer advisory" in row["warning"]
