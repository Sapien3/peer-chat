"""Tests for peer_watch.py: read-only polling, bounded recovery, safe signalling, idle exit."""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import peer_platform as pp  # noqa: E402
import peer_watch as pw  # noqa: E402
from peer_chat import Store  # noqa: E402

ME = os.getpid()
T1 = "11111111-1111-4111-8111-111111111111"
T2 = "22222222-2222-4222-8222-222222222222"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path_factory):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path_factory.mktemp("empty-home")))


@pytest.fixture
def root(tmp_path):
    r = tmp_path / "state"
    r.mkdir(mode=0o700)
    return r


def spawn_sleeper():
    child = subprocess.Popen([sys.executable, "-c", "import time; print('r', flush=True); time.sleep(30)"],
                             stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "r"
    return child


def dead_pid():
    p = subprocess.Popen([sys.executable, "-c", "pass"]); p.wait()
    return p.pid


def make_bridge(root, thread, *, owner_pid=ME, runtime_pid=None, supervision=True, stop=None, watch=None, codex_home=None):
    home = str(codex_home or (root.parent / "home"))
    store = Store(root / thread / "inbox.sqlite")
    try:
        store.put("config", {"thread": thread, "socket": str(root / thread / "l.sock"), "codex_home": home,
                             "owner_pid": owner_pid, "owner_identity": pp.process_identity(owner_pid) or "gone",
                             "supervision": supervision, "peer": {"pid": 1, "identity": "x", "socket": "/x", "name": "x"}})
        if runtime_pid is not None:
            store.put("runtime", {"pid": runtime_pid, "identity": pp.process_identity(runtime_pid) or "gone"})
        if stop is not None:
            store.put("stop", stop)
        if watch is not None:
            store.put("watch", watch)
    finally:
        store.close()
    return root / thread / "inbox.sqlite"


def read_meta(db_path, key):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row and row[0] else None
    finally:
        conn.close()


class Recorder:
    """Captures process actions instead of performing them (module indirections only)."""

    def __init__(self, monkeypatch, *, returncode=0, stderr=""):
        self.runs, self.popens, self.kills = [], [], []
        self.returncode, self.stderr = returncode, stderr

        def fake_run(cmd, **kw):
            self.runs.append((list(cmd), kw))
            return subprocess.CompletedProcess(cmd, self.returncode, stdout="", stderr=self.stderr)

        class FakePopen:
            def __init__(inner, cmd, **kw):
                self.popens.append((list(cmd), kw))
                inner.pid = 424242
        monkeypatch.setattr(pw, "_run", fake_run)
        monkeypatch.setattr(pw, "_popen", FakePopen)
        monkeypatch.setattr(pw, "_kill", lambda pid, sig: self.kills.append((pid, sig)))


# --------------------------------------------------------------------------
# read-only polling and scope
# --------------------------------------------------------------------------


def test_idle_tick_never_writes_and_skips_unsupervised_and_stopped(root, monkeypatch):
    rec = Recorder(monkeypatch)
    live = make_bridge(root, T1, runtime_pid=ME)                       # supervised, healthy
    unsup = make_bridge(root, T2, supervision=False)                   # not supervised, listener dead
    stopped = make_bridge(root, "33333333-3333-4333-8333-333333333333", stop=True)  # explicit stop, listener dead
    (root / "44444444-4444-4444-8444-444444444444").mkdir()            # no db
    (root / "notes.txt").write_text("x")
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (live, unsup, stopped)}
    summary = pw.tick(root, now=1000.0)
    assert summary == {"supervised": 1, "active": 1, "actions": [], "errors": 0}
    for p, (data, mtime) in before.items():
        assert p.read_bytes() == data and p.stat().st_mtime_ns == mtime, f"{p.parent.name} was written by an idle tick"
    assert rec.runs == [] and rec.popens == [] and rec.kills == []
    assert pw.tick(root / "missing", now=1000.0)["supervised"] == 0


def test_tick_skips_symlinked_foreign_and_corrupt_bridges(root, monkeypatch):
    rec = Recorder(monkeypatch)
    make_bridge(root, T1, runtime_pid=ME)
    (root / T2).symlink_to(root / T1)
    d = root / "33333333-3333-4333-8333-333333333333"; d.mkdir()
    (d / "inbox.sqlite").write_bytes(b"garbage" * 50)
    summary = pw.tick(root, now=1000.0)
    assert summary["supervised"] == 1 and summary["errors"] == 0 and rec.runs == []


# --------------------------------------------------------------------------
# recovery with bounded backoff
# --------------------------------------------------------------------------


def test_listener_exited_owner_alive_triggers_bounded_recovery(root, monkeypatch):
    rec = Recorder(monkeypatch, returncode=0)
    db = make_bridge(root, T1, runtime_pid=dead_pid())
    summary = pw.tick(root, now=1000.0)
    assert summary["actions"] == [(T1, "recover_listener_exited")]
    cmd, kw = rec.runs[0]
    assert cmd[1:3] == ["-m", "peer_chat"] and "--recover" in cmd and "--owner-pid" in cmd and str(ME) in cmd
    assert "--owner-identity" in cmd and pp.process_identity(ME) in cmd
    assert kw["env"]["CODEX_HOME"] == str(root.parent / "home")
    state = read_meta(db, "watch")
    assert state["attempts"] == 1 and state["last_result"] == "started" and state["recovered_at"] == 1000.0
    assert state["next_at"] == 1010.0, "a fast crash cannot restart before the backoff"
    assert read_meta(db, "restore_error") is None
    event = read_meta(db, "recovery_event")
    assert event["reason"] == "listener_exited" and event["attempt"] == 1


def test_attempt_is_persisted_before_the_subprocess_runs(root, monkeypatch):
    db = make_bridge(root, T1, runtime_pid=dead_pid())
    seen = {}

    def crashing_run(cmd, **kw):
        seen["state"] = read_meta(db, "watch")
        seen["event"] = read_meta(db, "recovery_event")
        raise OSError("watchdog crashed mid-attempt")
    monkeypatch.setattr(pw, "_run", crashing_run)
    pw.tick(root, now=1000.0)
    assert seen["state"]["attempts"] == 1 and seen["state"]["last_result"] == "attempting" and seen["state"]["next_at"] == 1010.0
    assert seen["event"]["attempt"] == 1
    after = read_meta(db, "watch")
    assert after["last_result"] == "failed" and after["last_error"] == "OSError" and after["attempts"] == 1
    assert pw.tick(root, now=1005.0)["actions"] == [], "no ambiguous immediate retry"


def test_crash_loop_cannot_restart_faster_than_backoff_and_resets_only_when_stable(root, monkeypatch):
    rec = Recorder(monkeypatch, returncode=0)
    db = make_bridge(root, T1, runtime_pid=dead_pid())
    now = 1000.0
    for i, delay in enumerate([10, 20, 40], start=1):
        assert pw.tick(root, now=now)["actions"] == [(T1, "recover_listener_exited")]
        assert read_meta(db, "watch")["attempts"] == i, "start succeeded but the listener died again: count kept"
        assert pw.tick(root, now=now + 1)["actions"] == []
        now += delay
    assert len(rec.runs) == 3
    # now the listener is genuinely alive: not reset until STABLE_S have passed since the last start
    last_start = now - 40  # the third start happened at 1030
    store = Store(db)
    store.put("runtime", {"pid": ME, "identity": pp.process_identity(ME)})
    store.close()
    pw.tick(root, now=last_start + pw.STABLE_S - 5)
    assert read_meta(db, "watch")["attempts"] == 3, "inside the stability window the count is kept"
    pw.tick(root, now=last_start + pw.STABLE_S + 5)
    state = read_meta(db, "watch")
    assert state["attempts"] == 0 and state["last_result"] == "recovered" and state["next_at"] == 0


def test_failed_recovery_backs_off_exponentially_then_pauses(root, monkeypatch):
    rec = Recorder(monkeypatch, returncode=1, stderr="boom")
    db = make_bridge(root, T1, runtime_pid=dead_pid())
    now = 1000.0
    for i, delay in enumerate([10, 20, 40, 80, 160], start=1):
        assert pw.tick(root, now=now)["actions"] == [(T1, "recover_listener_exited")], f"attempt {i}"
        state = read_meta(db, "watch")
        assert state["attempts"] == i and state["last_result"] == "failed" and state["last_error"] == "boom"
        assert state["next_at"] == now + delay and state["paused_until"] is None
        assert "recovery failed" in read_meta(db, "restore_error")
        assert pw.tick(root, now=now + delay - 1)["actions"] == [], "before next_at nothing happens"
        now += delay
    pw.tick(root, now=now)  # sixth attempt reaches MAX_ATTEMPTS
    state = read_meta(db, "watch")
    assert state["attempts"] == 6 and state["paused_until"] == 1000.0 + pw.WINDOW_S and state["next_at"] == state["paused_until"]
    assert len(rec.runs) == 6
    assert pw.tick(root, now=now + 1000)["actions"] == [] and len(rec.runs) == 6, "paused"
    pw.tick(root, now=1000.0 + pw.WINDOW_S + 1)  # window reset
    state = read_meta(db, "watch")
    assert state["attempts"] == 1 and state["paused_until"] is None and len(rec.runs) == 7


def test_backoff_delay_curve():
    assert [pw._backoff_delay(n) for n in range(1, 8)] == [10, 20, 40, 80, 160, 300, 300]


def test_success_after_failures_keeps_count_until_stable(root, monkeypatch):
    rec = Recorder(monkeypatch, returncode=1, stderr="x")
    db = make_bridge(root, T1, runtime_pid=dead_pid())
    pw.tick(root, now=1000.0)
    pw.tick(root, now=1010.0)
    assert read_meta(db, "watch")["attempts"] == 2
    rec.returncode = 0
    pw.tick(root, now=1030.0)
    state = read_meta(db, "watch")
    assert state["attempts"] == 3 and state["last_result"] == "started" and state["last_error"] is None
    assert read_meta(db, "restore_error") is None


def test_live_listener_after_failures_marks_recovered_without_action(root, monkeypatch):
    rec = Recorder(monkeypatch)
    db = make_bridge(root, T1, runtime_pid=ME, watch={"attempts": 3, "window_start": 900.0, "next_at": 2000.0,
                                                       "last_result": "started", "recovered_at": 900.0})
    summary = pw.tick(root, now=1000.0)
    assert summary["active"] == 1 and summary["actions"] == [] and rec.runs == []
    state = read_meta(db, "watch")
    assert state["attempts"] == 0 and state["last_result"] == "recovered" and state["paused_until"] is None


# --------------------------------------------------------------------------
# owner resume and owner offline
# --------------------------------------------------------------------------


def write_registration(root, thread, home, owner_pid, name="astra"):
    reg = root / "registry"
    reg.mkdir(exist_ok=True, mode=0o700)
    (reg / f"{thread}.json").write_text(json.dumps({"version": 1, "kind": "codex", "thread": thread, "codex_home": str(home),
                                                    "owner_pid": owner_pid, "owner_identity": pp.process_identity(owner_pid),
                                                    "name": name, "registered_at": 1.0, "updated_at": 1.0}))


def test_owner_resumed_signals_only_the_verified_old_listener_then_recovers(root, monkeypatch):
    rec = Recorder(monkeypatch)
    home = root.parent / "home"; home.mkdir(exist_ok=True)
    old_owner = spawn_sleeper()
    listener = spawn_sleeper()
    try:
        db = make_bridge(root, T1, owner_pid=old_owner.pid, runtime_pid=listener.pid, codex_home=home)
        old_owner.kill(); old_owner.wait()
        write_registration(root, T1, home, ME)  # the resumed owner is this process
        summary = pw.tick(root, now=1000.0)
        assert summary["actions"] == [(T1, "signal_old_listener")]
        assert rec.kills == [(listener.pid, signal.SIGTERM)], "only the listener, never the owner"
        assert rec.runs == []
        assert read_meta(db, "watch")["last_result"] == "signalled_old_listener"
        listener.kill(); listener.wait()
        summary = pw.tick(root, now=1001.0)
        assert summary["actions"] == [(T1, "recover_owner_resumed")]
        cmd, kw = rec.runs[0]
        assert "--owner-pid" in cmd and str(ME) in cmd and pp.process_identity(ME) in cmd
        assert kw["env"]["CODEX_HOME"] == str(home)
        assert read_meta(db, "recovery_event")["reason"] == "owner_resumed"
    finally:
        for p in (old_owner, listener):
            p.kill(); p.wait()


def test_identity_recheck_prevents_killing_a_recycled_pid(root, monkeypatch):
    rec = Recorder(monkeypatch)
    home = root.parent / "home"; home.mkdir(exist_ok=True)
    old_owner = spawn_sleeper()
    listener = spawn_sleeper()
    try:
        make_bridge(root, T1, owner_pid=old_owner.pid, runtime_pid=listener.pid, codex_home=home)
        old_owner.kill(); old_owner.wait()
        write_registration(root, T1, home, ME)
        real_identity = pp.process_identity
        calls = {"n": 0}

        def flapping(pid):
            value = real_identity(pid)
            if pid == listener.pid:
                calls["n"] += 1
                if calls["n"] >= 2:  # the recheck in _signal_listener sees a different process
                    return "recycled"
            return value
        monkeypatch.setattr(pw.pp, "process_identity", flapping)
        summary = pw.tick(root, now=1000.0)
        assert rec.kills == [], "identity changed between check and kill: no signal"
        assert summary["actions"] == []
    finally:
        for p in (old_owner, listener):
            p.kill(); p.wait()


def test_replacement_owner_must_match_thread_and_home_and_be_live(root, monkeypatch):
    rec = Recorder(monkeypatch)
    home = root.parent / "home"; home.mkdir(exist_ok=True)
    other_home = root.parent / "other"; other_home.mkdir(exist_ok=True)
    db = make_bridge(root, T1, owner_pid=dead_pid(), runtime_pid=dead_pid(), codex_home=home)
    write_registration(root, T1, other_home, ME)  # wrong home
    write_registration(root, T2, home, ME)        # wrong thread
    summary = pw.tick(root, now=1000.0)
    assert summary["actions"] == [(T1, "keep_endpoint_owner_offline")]
    assert rec.runs == [] and len(rec.popens) == 1 and "_serve" in rec.popens[0][0]
    assert read_meta(db, "watch")["last_result"] == "endpoint_kept"
    dead_owner = dead_pid()
    (root / "registry" / f"{T1}.json").write_text(json.dumps({"version": 1, "kind": "codex", "thread": T1, "codex_home": str(home),
                                                              "owner_pid": dead_owner, "owner_identity": "gone", "name": "n"}))
    pw.tick(root, now=2000.0)
    assert rec.runs == [], "a dead 'replacement' is not a replacement"


def test_owner_offline_endpoint_keep_is_capped_like_recovery(root, monkeypatch):
    rec = Recorder(monkeypatch)
    db = make_bridge(root, T1, owner_pid=dead_pid(), runtime_pid=dead_pid())
    now = 1000.0
    for i, delay in enumerate([10, 20, 40, 80, 160], start=1):
        pw.tick(root, now=now)
        assert len(rec.popens) == i
        state = read_meta(db, "watch")
        assert state["attempts"] == i and state["last_result"] == "endpoint_kept" and state["next_at"] == now + delay
        pw.tick(root, now=now + 1)
        assert len(rec.popens) == i
        now += delay
    pw.tick(root, now=now)
    state = read_meta(db, "watch")
    assert len(rec.popens) == 6 and state["paused_until"] == 1000.0 + pw.WINDOW_S
    pw.tick(root, now=now + 500)
    assert len(rec.popens) == 6, "paused: no more endpoint spawns this window"


def test_endpoint_attempt_persisted_before_spawn(root, monkeypatch):
    db = make_bridge(root, T1, owner_pid=dead_pid(), runtime_pid=dead_pid())
    seen = {}

    def crashing_popen(cmd, **kw):
        seen["state"] = read_meta(db, "watch")
        raise OSError("spawn failed")
    monkeypatch.setattr(pw, "_popen", crashing_popen)
    pw.tick(root, now=1000.0)
    assert seen["state"]["attempts"] == 1 and seen["state"]["last_result"] == "attempting_endpoint"
    assert read_meta(db, "watch")["last_result"] == "failed"


def test_owner_changed_between_check_and_recovery_is_skipped(root, monkeypatch):
    rec = Recorder(monkeypatch)
    owner = spawn_sleeper()
    try:
        db = make_bridge(root, T1, owner_pid=owner.pid, runtime_pid=dead_pid())
        real_identity = pp.process_identity
        seen = {"n": 0}

        def flapping(pid):
            if pid == owner.pid:
                seen["n"] += 1
                return real_identity(pid) if seen["n"] == 1 else None
            return real_identity(pid)
        monkeypatch.setattr(pw.pp, "process_identity", flapping)
        pw.tick(root, now=1000.0)
        assert rec.runs == []
        assert read_meta(db, "watch")["last_result"] == "skipped"
    finally:
        owner.kill(); owner.wait()


# --------------------------------------------------------------------------
# watchdog process: private files, idle exit, single instance
# --------------------------------------------------------------------------


def test_private_watch_files_refuse_symlink_and_foreign_owner(root, tmp_path, monkeypatch):
    victim = tmp_path / "victim"; victim.write_text("keep")
    (root / "watch.json").symlink_to(victim)
    assert pw._write_private_json(root / "watch.json", {"pid": 1}) is False
    assert victim.read_text() == "keep" and pw._read_private_json(root / "watch.json") is None
    (root / "watch.log").symlink_to(victim)
    log = pw._open_log(root)
    log.write(b"x"); log.close()
    assert victim.read_text() == "keep", "log never follows a symlink"
    real = root / "watch.lock"; real.write_text("")
    real_lstat = Path.lstat

    def foreign(self):
        st = real_lstat(self)
        if self.name == "watch.lock":
            class S:
                st_mode, st_uid = st.st_mode, os.getuid() + 1
            return S()
        return st
    monkeypatch.setattr(Path, "lstat", foreign)
    assert pw._private_path(real) is None
    monkeypatch.undo()


def test_log_is_capped_by_rotation(root):
    log_path = root / "watch.log"
    log_path.write_bytes(b"x" * (pw.LOG_CAP_BYTES + 1))
    log = pw._open_log(root)
    log.write(b"new\n"); log.close()
    assert (root / "watch.log.1").stat().st_size == pw.LOG_CAP_BYTES + 1
    assert log_path.read_bytes() == b"new\n"
    assert oct(log_path.stat().st_mode & 0o777) == "0o600"


def test_main_exits_when_nothing_supervised_and_records_status(root, monkeypatch):
    make_bridge(root, T1, runtime_pid=ME, supervision=False)
    clock = {"t": 1000.0}
    monkeypatch.setattr(pw.time, "time", lambda: clock["t"])

    def fake_sleep(s):
        clock["t"] += 30
    monkeypatch.setattr(pw.time, "sleep", fake_sleep)
    assert pw.main([str(root)]) == 0
    record = json.loads((root / "watch.json").read_text())
    assert record["pid"] == ME and record["identity"] == pp.process_identity(ME)
    assert record["exit_reason"] == "idle: nothing supervised" and record["ticks"] >= 2 and record["supervised"] == 0
    assert record["errors"] == {"count": 0, "last": None}
    assert oct((root / "watch.json").stat().st_mode & 0o777) == "0o600"


def test_main_keeps_running_while_supervised_then_stops_on_signal(root, monkeypatch):
    rec = Recorder(monkeypatch)
    make_bridge(root, T1, runtime_pid=ME)
    clock = {"t": 1000.0}
    monkeypatch.setattr(pw.time, "time", lambda: clock["t"])
    ticks = {"n": 0}

    def fake_sleep(s):
        clock["t"] += 30
        ticks["n"] += 1
        if ticks["n"] == 5:
            os.kill(os.getpid(), signal.SIGTERM)
    monkeypatch.setattr(pw.time, "sleep", fake_sleep)
    assert pw.main([str(root)]) == 0
    record = json.loads((root / "watch.json").read_text())
    assert record["exit_reason"] == "stopped" and record["supervised"] == 1 and record["active"] == 1 and record["ticks"] == 5
    assert rec.runs == []


def test_main_second_instance_yields_to_lock(root, monkeypatch):
    import fcntl
    holder = open(root / "watch.lock", "a")
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert pw.main([str(root)]) == 0
        assert not (root / "watch.json").exists(), "the yielding instance writes nothing"
    finally:
        holder.close()
    assert pw.main([]) == 2


def test_ensure_returns_live_record_or_starts(root, monkeypatch):
    (root / "watch.json").write_text(json.dumps({"pid": ME, "identity": pp.process_identity(ME)}))
    assert pw.ensure(root)["pid"] == ME
    (root / "watch.json").write_text(json.dumps({"pid": dead_pid(), "identity": "gone"}))
    rec = Recorder(monkeypatch)
    assert pw.ensure(root) == {"starting": 424242}
    cmd, kw = rec.popens[0]
    assert cmd[1:3] == ["-m", "peer_watch"] and cmd[3] == str(root) and kw["start_new_session"] is True


def test_watchdog_never_targets_owner_or_model_processes():
    src = (ROOT / "peer_watch.py").read_text()
    assert src.count("_kill(") == 1 and src.count("_kill = os.kill") == 1, "exactly one signalling site via the indirection"
    assert "owner_pid, signal" not in src and "SIGKILL" not in src
    assert "codex queue" not in src and "app-server" not in src
