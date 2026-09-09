"""Real-process feedback, crashes and verified thread resume; never start models."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

from test_codex_mesh import mesh, Owner, wait_message


def until(check, seconds=16):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.1)
    assert check()


def send_raw(a, root, target='B'):
    p = root / 'test-message.txt';p.write_text('Bounded test message')
    return a.command('send', '--to', target, '--file', str(p))


def test_send_reports_blocked_destination_and_all_status_without_thread(mesh):
    root, (a, b, _) = mesh
    a.command('connect', 'B')
    first = send_raw(a, root)
    assert first['model_received'] is False
    assert first['delivery_state'] == 'awaiting_lifecycle_hook'
    assert first['warning'] and first['transport_status'] == 'written'
    wait_message(b, first['id'])
    b.command('delivery', 'auto', '--budget', '0')
    second = send_raw(a, root)
    assert second['destination']['remaining'] == 0
    assert second['delivery_state'] == 'paused_budget'
    assert 'exhaust' in second['warning'].lower()
    env = dict(os.environ);env.pop('CODEX_THREAD_ID', None)
    command = [sys.executable, '-m', 'peer_chat', '--state-root', str(a.state), 'status']
    table = subprocess.run(command, env=env, capture_output=True, text=True, check=True).stdout
    assert 'PENDING' in table and 'B' in table and 'paused_budget' in table
    rows = json.loads(subprocess.run(command+['--json'], env=env, capture_output=True, text=True, check=True).stdout)
    assert {x['thread'] for x in rows} == {a.thread, b.thread}
    b.command('stop')
    failed = send_raw(a, root)
    assert failed['destination']['runtime_state'] == 'stopped'
    assert failed['warning'] and not failed['model_received']


def test_watch_recovers_killed_listener_without_refill_or_manual_start(mesh):
    root, (a, b, _) = mesh
    a.command('connect', 'B')
    b.command('delivery', 'auto', '--budget', '0')
    before = b.command('status')
    old_pid = before['runtime']['pid']
    os.kill(old_pid, signal.SIGKILL)
    def restored():
        row = b.command('status')
        return row if row['running'] and row['runtime']['pid'] != old_pid else None
    after = until(restored)
    assert after['remaining'] == 0 and after['wake_remaining'] == 0
    assert after['runtime']['socket'] == before['runtime']['socket']
    assert len(after['peers']) == 1
    mid = send_raw(a, root)['id'];wait_message(b, mid)
    b.command('stop')
    time.sleep(2.5)
    assert b.command('status')['runtime_state'] == 'stopped'


def test_watch_resumes_same_thread_before_any_hook_or_registration(mesh):
    root, owners = mesh
    a, old, _ = owners
    a.command('connect', 'B')
    old.command('delivery', 'auto', '--budget', '0')
    before = old.command('status')
    old.proc.terminate();old.proc.wait(timeout=5)
    old.proc.stdin.close();old.proc.stdout.close()
    replacement = Owner(old.thread, old.home, old.state, Path(__file__).resolve().parents[1])
    owners[1] = replacement
    def restored():
        status = replacement.command('status')
        return status if status['running'] and status['owner_pid'] == replacement.proc.pid else None
    status = until(restored)
    assert status['runtime']['socket'] == before['runtime']['socket']
    assert status['remaining'] == 0 and status['phase'] == 'unknown' and status['hook_seen'] is None
    assert len(status['peers']) == 1
    sent = send_raw(a, root)
    assert sent['destination']['owner_pid'] == replacement.proc.pid
    assert wait_message(replacement, sent['id'])[2] == 'received'


def test_native_blocked_send_gets_bounded_advisory_without_budget_refill(mesh):
    root, (a, _, _) = mesh
    native = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    target = str(root / 'native.sock');native.bind(target);native.listen(8);native.settimeout(6)
    try:
        runtime = a.command('start', '--peer-pid', str(os.getpid()), '--peer-socket', target,
                            '--peer-name', 'Native tester', '--delivery', 'auto', '--budget', '0')
        # An untouched tab may have no saved permission metadata at all.
        import sqlite3
        with sqlite3.connect(a.home / 'state_5.sqlite') as db:
            db.execute('DELETE FROM threads WHERE id=?', (a.thread,))
        mid = str(uuid.uuid4());address = 'uds:' + target
        body = f'<cross-session-message from="{address}" from-mode="bypass">\nhello\n</cross-session-message>'
        wire = (json.dumps({'type':'user','msg_id':mid,'from':address,'message':{'role':'user','content':body}})+'\n').encode()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sender:
            sender.connect(runtime['socket']);sender.sendall(wire)
        received, _ = native.accept()
        with received:
            data = b''
            while not data.endswith(b'\n'):
                data += received.recv(65536)
        frame = json.loads(data);text = frame['message']['content']
        assert 'PEER CHAT DELIVERY NOTICE' in text and mid in text and 'paused_budget' in text
        assert 'from-name="peer-chat-bridge"' in text and 'from-mode="prompting"' in text
        assert a.command('status')['remaining'] == 0
        # A second message in the SAME blocked state must get its own warning,
        # rather than disappearing behind state-transition coalescing.
        second, third = str(uuid.uuid4()), str(uuid.uuid4())
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sender:
            sender.connect(runtime['socket']);sender.sendall(wire.replace(mid.encode(), second.encode()) + wire.replace(mid.encode(), third.encode()))
        received, _ = native.accept()
        with received:
            data = b''
            while not data.endswith(b'\n'):
                data += received.recv(65536)
        assert second in json.loads(data)['message']['content'] and third in json.loads(data)['message']['content']
        native.settimeout(1.5)
        try:
            extra, _ = native.accept();extra.close();assert False, 'unchanged state notice repeated'
        except socket.timeout:
            pass
    finally:
        native.close()


def test_codex_notice_is_metadata_only_and_does_not_spend_or_wake(mesh):
    root, (a, b, _) = mesh
    a.command('connect', 'B')
    # Establish a blocked pending message; B's listener reports its own state
    # over a control frame, not as another task requiring a reply.
    b.command('delivery', 'auto', '--budget', '0')
    mid = send_raw(a, root)['id'];wait_message(b, mid)
    def notice():
        s = a.store()
        try: return s.get('peer_notices')
        finally: s.close()
    data = until(notice)
    assert any(v['state'] == 'paused_budget' and v['remaining'] == 0 for v in data.values())
    s = a.store()
    try:
        assert not s.pending()
        assert s.get('remaining') == 'unlimited'
        assert s.get('wake_pending') is None
        from peer_delivery import take_notices
        assert 'PEER CHAT DELIVERY NOTICE' in take_notices(s)
        assert take_notices(s) == ''
    finally: s.close()
