"""Black-box lifecycle and stream tests; no model/vendor calls."""
import concurrent.futures
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


def test_stream_burst_past_old_lifetime_cap_retains_all_messages_and_receipts(bridge):
    cli, start, runtime, wire, connect, root = bridge
    ids = [str(uuid.uuid4()) for _ in range(1200)]
    started = time.monotonic()
    def burst(batch):
        with connect() as client:
            client.sendall(b''.join(wire('burst', mid=mid) for mid in batch))
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(burst, [ids[i:i + 150] for i in range(0, len(ids), 150)]))
    eventually(lambda: len(cli('read')) == len(ids), seconds=15)
    rows = cli('read', '--ack')
    assert {r['id'] for r in rows} == set(ids)
    with connect() as client:
        client.sendall(wire('duplicate', mid=ids[0]) + wire('after acknowledged history'))
        address = json.loads(wire())['from']
        client.sendall((json.dumps({'type': 'control', 'action': 'peer_message_status', 'orig_msg_id': ids[0], 'status': 'delivered', 'from': address}) + '\n').encode())
    eventually(lambda: len(cli('read')) == 1201)
    state = next((root / 'state').glob('*/inbox.sqlite'))
    def receipt_received():
        with sqlite3.connect(state) as db:
            return db.execute("SELECT count(*) FROM messages WHERE kind='receipt'").fetchone()[0] == 1
    eventually(receipt_received)
    assert cli('status')['rejected'] == 0
    print(f'1200 durable socket arrivals plus follow-ups in {time.monotonic() - started:.3f}s')


def test_unlimited_default_and_explicit_window_survive_restart(bridge):
    cli, start, runtime, wire, connect, root = bridge
    assert cli('status')['remaining'] == 'unlimited'
    cli('restart')
    assert cli('status')['remaining'] == 'unlimited'
    cli('delivery', 'auto', '--budget', '3')
    cli('restart')
    assert cli('status')['remaining'] == 3


def test_connect_upgrades_old_listener_preserving_finite_budget(bridge):
    cli, start, runtime, wire, connect, root = bridge
    cli('delivery', 'auto', '--budget', '7')
    database = next((root / 'state').glob('*/inbox.sqlite'))
    with sqlite3.connect(database) as db:
        saved = json.loads(db.execute("SELECT value FROM meta WHERE key='runtime'").fetchone()[0])
        saved['protocol_version'] = 3
        db.execute("UPDATE meta SET value=? WHERE key='runtime'", (json.dumps(saved),))
    replacement = start()
    assert replacement['pid'] != runtime['pid'] and replacement['protocol_version'] == 6
    assert cli('status')['remaining'] == 7
    assert cli('delivery', 'auto', '--budget', 'unlimited')['budget'] == 'unlimited'


@pytest.mark.parametrize('terminal', ['task_complete', 'turn_aborted'])
def test_listener_reconciles_terminal_state_without_owner_prompt(bridge, terminal):
    from datetime import datetime, timezone
    cli, start, runtime, wire, connect, root = bridge
    database = next((root / 'state').glob('*/inbox.sqlite'))
    rollout = root / 'interrupted.jsonl'
    turn = str(uuid.uuid4())
    at = time.time() - 10
    rollout.write_text(json.dumps({'timestamp': datetime.fromtimestamp(at + 2, timezone.utc).isoformat(),
        'type': 'event_msg', 'payload': {'type': terminal, 'turn_id': turn}}) + '\n')
    native = root / 'queue-fixture'
    queue_log = root / 'queue.jsonl'
    native.write_text(f'#!{sys.executable}\nimport json,sys\nwith open({str(queue_log)!r}, "a") as f: f.write(json.dumps(sys.argv[1:]) + "\\n")\n')
    native.chmod(0o700)
    with sqlite3.connect(root / 'codex/state_5.sqlite') as db:
        db.execute('ALTER TABLE threads ADD COLUMN rollout_path TEXT')
        db.execute('UPDATE threads SET rollout_path=?', (str(rollout),))
    with sqlite3.connect(database) as db:
        config = json.loads(db.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        config['codex_bin'] = str(native)
        seen = {'event': 'UserPromptSubmit', 'routing': 'matched', 'turn': turn,
                'at': at, 'owner_identity': config['owner_identity']}
        for key, value in (('config', config), ('phase', 'active'), ('hook_seen', seen)):
            db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, json.dumps(value)))
    cli('delivery', 'auto', '--budget', '40')
    mid = str(uuid.uuid4())
    with connect() as client:
        client.sendall(wire('retained message after interruption', mid=mid))
    if terminal == 'turn_aborted':
        eventually(lambda: cli('status')['delivery_state'] == 'paused_interrupted')
        time.sleep(1.2)
        assert not queue_log.exists()
        assert cli('read')[0]['status'] == 'received'
        assert cli('status')['wake_remaining'] == 40
        return
    eventually(lambda: queue_log.exists())
    eventually(lambda: cli('status')['wake_evidence']['last']['state'] == 'queued')
    status = cli('status')
    assert status['phase'] == 'idle' and status['phase_evidence']['event'] == 'task_complete'
    assert status['remaining'] == 40 and status['wake_remaining'] == 39
    assert status['hook_seen']['event'] == 'UserPromptSubmit'
    assert cli('read')[0]['status'] == 'received'
    time.sleep(1.2)  # Several dispatcher ticks must not queue the same notice again.
    notices = queue_log.read_text().splitlines()
    assert len(notices) == 1 and mid in notices[0]
    assert 'retained message after interruption' not in notices[0]
