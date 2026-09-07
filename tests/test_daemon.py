"""Black-box lifecycle and stream tests; no model/vendor calls."""
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "src" / "peer_chat.py"


def eventually(check, seconds=8):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if check():
            return
        time.sleep(.05)
    assert check()


@pytest.fixture
def bridge():
    with tempfile.TemporaryDirectory(prefix="pc-") as temp:
        root = Path(temp)
        home = root / "codex"
        home.mkdir()
        thread = str(uuid.uuid4())
        with sqlite3.connect(home / "state_5.sqlite") as db:
            db.execute("CREATE TABLE threads(id TEXT,sandbox_policy TEXT,approval_mode TEXT,archived INTEGER)")
            db.execute("INSERT INTO threads VALUES (?,?,?,0)", (thread, '{"type":"disabled"}', "never"))
        env = dict(os.environ, CODEX_HOME=str(home), CODEX_THREAD_ID=thread)
        target = root / "p.sock"
        peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        peer.bind(str(target))
        peer.listen(5)
        base = [sys.executable, str(SCRIPT), "--state-root", str(root / "state")]

        def cli(*args):
            if args == ('status',):
                args = ('status', '--current')
            r = subprocess.run(base + list(args), env=env, capture_output=True, text=True, timeout=16)
            assert r.returncode == 0, r.stderr
            return json.loads(r.stdout)

        def start(owner=os.getpid()):
            return cli("start", "--peer-pid", str(os.getpid()), "--peer-socket", str(target),
                       "--owner-pid", str(owner), "--socket-dir", str(root / "s"))

        runtime = start()

        def wire(body="hello", mid=None):
            address = "uds:" + str(target)
            text = f'<cross-session-message from="{address}" from-mode="bypass">\n{body}\n</cross-session-message>'
            return (json.dumps({"type": "user", "msg_id": mid or str(uuid.uuid4()), "from": address,
                               "message": {"role": "user", "content": text}}) + "\n").encode()

        def connect():
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect(runtime["socket"])
            return s

        yield cli, start, runtime, wire, connect, root
        cli("stop")
        peer.close()


def test_stream_damage_dedup_crash_restart_and_private_storage(bridge):
    cli, start, runtime, wire, connect, root = bridge
    mid = str(uuid.uuid4())
    with connect() as s:
        s.sendall(b'bad json\n[]\n{"type":"auth","token":"DO-NOT-STORE"}\n')
        good = wire(mid=mid)
        s.sendall(good[:9])
        time.sleep(.05)
        s.sendall(good[9:])
        s.sendall(good)
    eventually(lambda: len(cli("read")) == 1)
    with connect() as s:
        s.sendall(wire("unterminated")[:-1])
    with connect() as s:
        s.sendall(b"x" * 65537)
    time.sleep(.3)
    assert len(cli("read", "--ack")) == 1
    assert cli("read")[0]["status"] == "consumed"
    assert cli("status")["rejected"] >= 3
    os.kill(runtime["pid"], signal.SIGKILL)
    eventually(lambda: not cli("status")["running"])
    replacement = start()
    assert replacement["socket"] == runtime["socket"]
    assert replacement["pid"] != runtime["pid"]
    with connect() as s:
        s.sendall(wire(mid=mid))
        s.sendall(wire("after restart"))
    eventually(lambda: len(cli("read")) == 2)
    assert cli("read")[0]["status"] == "consumed"
    for path in (root / "state").rglob("*"):
        if path.is_file():
            assert b"DO-NOT-STORE" not in path.read_bytes()
            assert path.stat().st_mode & 0o077 == 0


def test_empty_codex_metadata_allows_receive_but_never_guesses_sender_permissions(bridge):
    cli, start, runtime, wire, connect, root = bridge
    cli("stop")
    (root / "codex/state_5.sqlite").rename(root / "codex/state.backup")
    started = start()
    assert started["socket"] == runtime["socket"]
    with connect() as client:
        client.sendall(wire("Claude initiated into an empty Codex session"))
    eventually(lambda: len(cli("read")) == 1)
    message = root / "outgoing.txt"
    message.write_text("Never infer a permission class")
    sent = cli("send", "--file", str(message))
    assert sent["status"] == "uncertain"
    assert "Cannot determine" in sent["detail"]


def test_resume_restores_dead_owner_transport_and_preserves_inbox(bridge, monkeypatch):
    from peer_registry import register
    cli, start, runtime, wire, connect, root = bridge
    cli("stop")
    old_owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        start(old_owner.pid)
        with connect() as client:
            client.sendall(wire("durable across owner resume"))
        eventually(lambda: len(cli("read")) == 1)
        old_owner.terminate()
        old_owner.wait(timeout=5)
        eventually(lambda: cli("status")["runtime_state"] == "owner_offline")
        thread = next((root / "state").glob('*/inbox.sqlite')).parent.name
        monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
        record = register({"session_id": thread, "cwd": str(root)}, root / "codex", root / "state", owner_pid=os.getpid())
        assert record
        restored = cli("_restore", "--owner-pid", str(os.getpid()))
        assert restored["restored"] is True
        assert cli("status")["running"]
        assert cli("status")["runtime"]["socket"] == runtime["socket"]
        assert len(cli("read")) == 1
    finally:
        if old_owner.poll() is None:
            old_owner.kill()
            old_owner.wait()


def test_slow_connections_do_not_block_another_peer_message(bridge):
    cli, _, _, wire, connect, _ = bridge
    slow = connect()
    slow.sendall(b'{"')
    try:
        with connect() as s:
            s.sendall(wire("parallel delivery"))
        eventually(lambda: len(cli("read")) == 1, seconds=2)
        time.sleep(5.2)
        assert slow.recv(1) == b""
    finally:
        slow.close()


def test_supervised_daemon_reports_owner_exit_and_keeps_endpoint(bridge):
    cli, start, _, _, _, _ = bridge
    cli("stop")
    owner = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
    try:
        runtime = start(owner.pid)
        owner.terminate()
        owner.wait(timeout=5)
        eventually(lambda: cli("status")["runtime_state"] == 'owner_offline')
        assert Path(runtime["socket"]).exists()
        assert cli("status")["reachable"] is False
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait()


def test_uninvited_process_is_rejected_and_channel_still_works(bridge):
    cli, _, runtime, wire, connect, _ = bridge
    subprocess.run([sys.executable, "-c", "import socket,sys;s=socket.socket(socket.AF_UNIX);s.connect(sys.argv[1]);s.sendall(sys.argv[2].encode());s.close()",
                    runtime["socket"], wire("wrong process").decode()], check=True)
    eventually(lambda: cli("status")["rejected"] >= 1)
    assert cli("read") == []
    with connect() as s:
        s.sendall(wire("invited process"))
    eventually(lambda: len(cli("read")) == 1)


def test_restart_and_mode_changes_preserve_exhausted_budget(bridge):
    cli, start, runtime, _, _, _ = bridge
    cli("delivery", "queue", "--budget", "0")
    replacement = cli("restart")
    assert replacement["socket"] == runtime["socket"]
    assert cli("status")["remaining"] == 0
    cli("delivery", "inbox")
    cli("delivery", "queue")
    assert cli("status")["remaining"] == 0
    cli("stop")
    start()
    state = cli("status")
    assert state["remaining"] == 0 and state["delivery"] == "queue"


def test_reconnect_new_owner_drops_old_idle_evidence_and_keeps_inbox(bridge):
    """A discovered replacement process cannot inherit an old Stop hook."""
    cli, start, runtime, wire, connect, root = bridge
    mid = str(uuid.uuid4())
    with connect() as client:
        client.sendall(wire("retained across owner replacement", mid))
    eventually(lambda: cli("status")["messages"])
    cli("stop")
    database = next((root / "state").glob("*/inbox.sqlite"))
    with sqlite3.connect(database) as db:
        for key, value in {"phase": "idle", "hook_seen": {"event": "Stop"},
                           "wake_pending": {"status": "queued", "message_id": mid}, "delivery": "auto"}.items():
            db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        start(owner=child.pid)
        result = cli("status")
        assert result["running"] is True
        assert result["phase"] == "unknown" and result["hook_seen"] is None
        assert result["wake"] is None and result["worker_error"] is None
        assert result["messages"] == [{"status": "received", "count": 1}]
        with sqlite3.connect(database) as db:
            assert db.execute("SELECT id FROM messages WHERE status='received'").fetchone()[0] == mid
    finally:
        cli("stop")
        child.terminate()
        child.wait(timeout=5)


def test_restart_preserves_configured_window_and_remaining_allowance(bridge):
    cli, _, _, _, _, _ = bridge
    cli("delivery", "auto", "--budget", "12")
    root = bridge[-1]
    database = next((root / "state").glob("*/inbox.sqlite"))
    with sqlite3.connect(database) as db:
        db.execute("INSERT OR REPLACE INTO meta VALUES ('remaining','4')")
        db.execute("INSERT OR REPLACE INTO meta VALUES ('wake_remaining','2')")
    cli("restart")
    result = cli("status")
    assert result["budget_limit"] == 12
    assert result["remaining"] == 4 and result["wake_remaining"] == 2


def test_resume_keeps_hook_evidence_already_written_by_new_owner(bridge):
    from peer_platform import process_identity
    cli, start, _, _, _, root = bridge
    cli('stop')
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    try:
        database = next((root/'state').glob('*/inbox.sqlite'))
        seen = {'event':'PostToolUse','routing':'matched','at':time.time(),
                'owner_identity':process_identity(child.pid),'turn':'new-owner-turn'}
        with sqlite3.connect(database) as db:
            db.execute("INSERT OR REPLACE INTO meta VALUES ('phase','\"active\"')")
            db.execute("INSERT OR REPLACE INTO meta VALUES ('hook_seen',?)", (json.dumps(seen),))
        start(owner=child.pid)
        status = cli('status')
        assert status['phase'] == 'active'
        assert status['hook_seen']['turn'] == 'new-owner-turn'
    finally:
        cli('stop');child.terminate();child.wait(timeout=5)
