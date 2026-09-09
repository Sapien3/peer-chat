"""Real owners, locks and Unix listeners; no model or vendor calls."""
import json
import os
from pathlib import Path
import socket
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import pytest

from peer_chat import Store
from peer_peers import all_peers
from peer_platform import process_identity

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="real /proc writer-lock harness")


class Owner:
    def __init__(self, thread, home, state, cwd):
        self.thread, self.home, self.state = thread, home, state
        self.env = dict(os.environ, CODEX_THREAD_ID=thread, CODEX_HOME=str(home))
        script = """import fcntl,json,subprocess,sys,os
open('/proc/self/comm','w').write('codex')
lock=open(sys.argv[1],'a');fcntl.flock(lock,fcntl.LOCK_EX)
print('ready',flush=True)
for line in sys.stdin:
 args=json.loads(line)
 try:
  result=subprocess.run(args,capture_output=True,text=True,timeout=25)
  print(json.dumps({'code':result.returncode,'stdout':result.stdout,'stderr':result.stderr}),flush=True)
 except Exception as exc:print(json.dumps({'code':1,'stderr':type(exc).__name__}),flush=True)
"""
        self.proc = subprocess.Popen([sys.executable, "-c", script, str(home / "thread-writer-locks" / f"{thread}.lock")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=self.env, cwd=cwd)
        assert self.proc.stdout.readline().strip() == "ready"

    def command(self, *args, okay=True):
        if args == ('status',):
            args = ('status', '--current')
        command = [sys.executable, "-m", "peer_chat", "--state-root", str(self.state), *args]
        self.proc.stdin.write(json.dumps(command) + "\n"); self.proc.stdin.flush()
        result = json.loads(self.proc.stdout.readline())
        if okay:
            assert result["code"] == 0, result.get("stderr")
            return json.loads(result["stdout"])
        return result

    def store(self):
        return Store(self.state / self.thread / "inbox.sqlite")

    def close(self):
        try:
            self.command("stop", okay=False)
        finally:
            self.proc.stdin.close(); self.proc.wait(timeout=5); self.proc.stdout.close()


def wait_message(owner, mid):
    until = time.monotonic() + 5
    while time.monotonic() < until:
        with sqlite3.connect(owner.state / owner.thread / "inbox.sqlite") as db:
            row = db.execute("SELECT peer,body,status FROM messages WHERE id=? AND kind='message'", (mid,)).fetchone()
        if row:
            return row
        time.sleep(.05)
    pytest.fail("message did not reach the expected inbox")


@pytest.fixture
def mesh():
    with tempfile.TemporaryDirectory(prefix="pc-mesh-") as tmp:
        root = Path(tmp); home = root / "home"; state = root / "state"
        (home / "thread-writer-locks").mkdir(parents=True)
        home.chmod(0o700)
        owners = []
        try:
            for name in ("A", "B", "C"):
                tid = str(uuid.uuid4())
                with sqlite3.connect(home / "state_5.sqlite") as db:
                    db.execute("CREATE TABLE IF NOT EXISTS threads(id TEXT PRIMARY KEY,cwd TEXT,model TEXT,rollout_path TEXT,sandbox_policy TEXT,approval_mode TEXT,archived INTEGER)")
                    db.execute("INSERT INTO threads VALUES (?,?,?,?,?,?,0)",
                        (tid,str(root),"test-model",str(root/f"{tid}.jsonl"),'{"type":"disabled"}',"never"))
                owner = Owner(tid, home, state, Path(__file__).resolve().parents[1]); owners.append(owner)
                owner.command("register", "--name", name)
            yield root, owners
        finally:
            # Detached watchdogs can recreate metadata during rmtree. Stop and
            # wait for every fixture writer before removing its temporary root.
            watch_path = state / "watch.json"
            if watch_path.exists():
                watch = json.loads(watch_path.read_text())
                if process_identity(watch['pid']) == watch['identity']:
                    os.kill(watch['pid'], signal.SIGTERM)
                    deadline = time.monotonic() + 10
                    while process_identity(watch['pid']) == watch['identity']:
                        assert time.monotonic() < deadline, 'fixture watchdog did not stop'
                        time.sleep(.05)
            runtimes = []
            for owner in owners:
                if (state / owner.thread / 'inbox.sqlite').exists():
                    store = owner.store()
                    try:
                        runtime = store.get('runtime')
                        if runtime:
                            runtimes.append(runtime)
                    finally:
                        store.close()
                owner.close()
            deadline = time.monotonic() + 10
            for runtime in runtimes:
                while process_identity(runtime['pid']) == runtime['identity']:
                    assert time.monotonic() < deadline, 'fixture listener did not stop'
                    time.sleep(.05)


def send(owner, root, body, *options):
    file = root / f"message-{uuid.uuid4()}.txt"; file.write_text(body)
    result = owner.command("send", "--file", str(file), *options)
    assert result["transport_status"] == "written", result
    return result["id"]


def test_codex_mesh_keeps_claude_and_routes_reply_to_its_sender(mesh):
    root, (a, b, c) = mesh
    native = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    native.bind(str(root / "claude.sock")); native.listen(20); native.settimeout(.1)
    stopped = threading.Event(); native_messages = []
    def receive():
        while not stopped.is_set():
            try: conn, _ = native.accept()
            except socket.timeout: continue
            with conn:
                conn.settimeout(2)
                data = b""
                while not data.endswith(b"\n"):
                    chunk = conn.recv(65536)
                    if not chunk: break
                    data += chunk
                if data: native_messages.append(json.loads(data))
    receiver = threading.Thread(target=receive); receiver.start()
    try:
        a.command("start", "--peer-pid", str(os.getpid()), "--peer-socket", str(root / "claude.sock"),
                  "--peer-name", "Claude reviewer", "--socket-dir", str(root / "s"))
        original = a.store()
        legacy = original.get("config")["peer"]
        original.close()
        assert a.command("connect", "B")["connected"] is True
        assert a.command("connect", "C")["connected"] is True
        store = a.store()
        peers = all_peers(store.get("config"))
        assert len(peers) == 3 and store.get("config")["peer"] == legacy
        store.close()
        to_a = send(b, root, "B asks A for review", "--to", "A")
        b_key, body, status = wait_message(a, to_a)
        assert body == "B asks A for review" and status == "received"
        reply = send(a, root, "A replies specifically to B", "--reply-to", to_a)
        assert wait_message(b, reply)[1] == "A replies specifically to B"
        c_store = c.store()
        assert not c_store.db.execute("SELECT 1 FROM messages WHERE id=?", (reply,)).fetchone()
        c_store.close()
        native_id = send(a, root, "Claude connection still works", "--to", "Claude reviewer")
        end = time.monotonic() + 3
        while not native_messages and time.monotonic() < end: time.sleep(.02)
        assert native_messages[0]["msg_id"] == native_id
        assert b_key.startswith("codex:")
    finally:
        stopped.set(); receiver.join(timeout=3); native.close()


def test_counterpart_daemon_restart_keeps_enrollment_and_rejects_uninvited_sender(mesh):
    root, (a, b, c) = mesh
    connection = a.command("connect", "B")
    assert connection["recipient_phase"] == "unknown"
    assert connection["model_delivery"] == "awaiting_lifecycle_hook"
    store = a.store(); before = all_peers(store.get("config")); store.close()
    first = b.command("status")["runtime"]["pid"]
    second = b.command("restart")["pid"]
    assert first != second
    mid = send(a, root, "survives daemon restart", "--to", "B")
    assert wait_message(b, mid)[1] == "survives daemon restart"
    store = a.store(); assert all_peers(store.get("config")) == before; store.close()
    status = b.command("status")
    rejected_before = status["rejected"]
    attacker = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    attacker.connect(status["runtime"]["socket"])
    attacker.close()
    end = time.monotonic() + 3
    while time.monotonic() < end:
        status = b.command("status")
        if status["rejected"] > rejected_before: break
        time.sleep(.02)
    assert status["rejected"] > rejected_before
    assert len(b.command("read")) == 1


def test_hooks_offer_messages_from_both_codex_peers(mesh, monkeypatch):
    import peer_hooks
    root, (a, b, c) = mesh
    a.command("connect", "B")
    a.command("connect", "C")
    first = send(b, root, "review from B", "--to", "A")
    second = send(c, root, "review from C", "--to", "A")
    wait_message(a, first); wait_message(a, second)
    monkeypatch.setenv("CODEX_HOME", str(a.home))
    monkeypatch.setenv("CODEX_THREAD_ID", a.thread)
    monkeypatch.setattr(__import__("peer_platform"),
                        "find_owner_pid", lambda *args, **kwargs: a.proc.pid)
    payload = {"hook_event_name": "PostToolUse", "session_id": a.thread,
               "transcript_path": str(root / f"{a.thread}.jsonl"), "turn_id": "test-tool-boundary"}
    result = peer_hooks.deliver(payload, a.state)
    text = result["hookSpecificOutput"]["additionalContext"]
    assert first in text and second in text
    assert "Invited peer: B" in text and "Invited peer: C" in text
    assert peer_hooks.deliver(payload, a.state) == {}
    assert a.command("status")["remaining"] == "unlimited"
    assert a.command("ack", first)["acknowledged"] == 1
    assert a.command("ack", second)["acknowledged"] == 1


def test_codex_reply_hops_count_repeated_visits(mesh):
    root, (a, b, _) = mesh
    a.command("connect", "B")
    mid = send(a, root, "first hop", "--to", "B")
    wait_message(b, mid)
    reply = send(b, root, "second hop", "--reply-to", mid)
    wait_message(a, reply)
    third = send(a, root, "third hop revisits A", "--reply-to", reply)
    wait_message(b, third)
    store = b.store()
    hops = json.loads(store.db.execute("SELECT hops FROM messages WHERE id=?", (third,)).fetchone()[0])
    store.close()
    assert len(hops) == 3 and hops[0] == hops[2]


def test_same_message_id_from_two_peers_requires_explicit_ack_sender(mesh):
    root, (a, b, c) = mesh
    a.command("connect", "B")
    a.command("connect", "C")
    mid = str(uuid.uuid4())
    store = a.store()
    records = all_peers(store.get("config"))
    for peer in records.values():
        store.accept(peer["key"], {"id": mid, "kind": "message", "body": "same id, different sender",
                                 "sender_mode": "prompting", "hops": []})
    store.close()
    result = a.command("ack", mid, okay=False)
    assert result["code"] != 0 and "ambiguous" in result["stderr"]
    assert a.command("ack", mid, "--from", "B")["acknowledged"] == 1
    rows = a.command("read", "--id", mid)
    assert sorted(r["status"] for r in rows) == ["consumed", "received"]
