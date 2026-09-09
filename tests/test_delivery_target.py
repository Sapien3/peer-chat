"""Renewing a receiving window must identify and affect the intended session."""
import json
import os
import sqlite3
import socket
import subprocess
import sys

import pytest

from test_observe import make_bridge, T1, T2
from peer_peers import codex_key


@pytest.fixture
def pair(tmp_path):
    key = codex_key('/h', T2)
    peers = {key: {'kind': 'codex', 'key': key, 'thread': T2, 'codex_home': '/h',
                       'name': 'receiver', 'state_root': str(tmp_path)}}
    sockets = []
    for name in ('a.sock', 'b.sock'):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(str(tmp_path / name));sock.listen(1);sockets.append(sock)
    a = make_bridge(tmp_path, T1, remaining=0, wake_remaining=0, peers=peers, socket_path=str(tmp_path / 'a.sock'))
    b = make_bridge(tmp_path, T2, remaining=0, wake_remaining=0, messages=('received',), socket_path=str(tmp_path / 'b.sock'))
    registry = tmp_path / 'registry';registry.mkdir()
    for tid, name in ((T1, 'sender'), (T2, 'receiver')):
        (registry / (tid + '.json')).write_text(json.dumps({'name': name}))
    yield tmp_path, a, b
    for sock in sockets:
        sock.close()


def run(root, *args, caller_thread=T1):
    env = dict(os.environ)
    env.pop('CODEX_THREAD_ID', None)
    if caller_thread:
        env['CODEX_THREAD_ID'] = caller_thread
    return subprocess.run([sys.executable, '-m', 'peer_chat', '--state-root', str(root), *args],
                          env=env, capture_output=True, text=True)


def remaining(path):
    with sqlite3.connect(path) as db:
        return json.loads(db.execute("SELECT value FROM meta WHERE key='remaining'").fetchone()[0])


def test_local_renewal_identifies_its_scope_and_still_paused_recipient(pair):
    root, a, b = pair
    before = b.read_bytes()
    result = run(root, 'delivery', 'auto', '--budget', '12')
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data['thread'] == T1 and data['scope'] == 'incoming_only'
    assert data['budget'] == data['wake_budget'] == data['budget_limit'] == 12
    assert data['blocked_recipients'][0]['thread'] == T2
    assert data['blocked_recipients'][0]['remaining'] == 0
    assert '--to NAME' in data['warning'] and 'SECRET' not in result.stdout
    assert remaining(a) == 12 and b.read_bytes() == before


@pytest.mark.parametrize('caller_thread', [T1, None])
def test_explicit_receiver_renewal_does_not_touch_callers_window_or_messages(pair, caller_thread):
    root, a, b = pair
    before = a.read_bytes()
    result = run(root, 'delivery', 'auto', '--budget', '12', '--to', 'receiver', caller_thread=caller_thread)
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data['thread'] == T2 and data['delivery_state'] == 'active_hooks'
    assert remaining(b) == 12 and a.read_bytes() == before
    with sqlite3.connect(b) as db:
        assert db.execute('SELECT status FROM messages').fetchone()[0] == 'received'


@pytest.mark.parametrize('args', [
    ['delivery','auto','--budget','12','--to','missing'],
    ['--thread',T1,'delivery','auto','--budget','12','--to','receiver'],
    ['--thread='+T1,'delivery','auto','--budget','12','--to','receiver'],
])
def test_invalid_or_conflicting_target_changes_neither_window(pair, args):
    root, a, b = pair
    before = (a.read_bytes(), b.read_bytes())
    result = run(root, *args)
    assert result.returncode != 0
    assert (a.read_bytes(), b.read_bytes()) == before


def test_ambiguous_name_changes_neither_window(pair):
    root, a, b = pair
    (root / 'registry' / (T1 + '.json')).write_text(json.dumps({'name': 'receiver'}))
    result = run(root, 'delivery', 'auto', '--budget', '12', '--to', 'receiver')
    assert result.returncode != 0 and 'ambiguous' in result.stderr
    assert remaining(a) == remaining(b) == 0


def test_unlimited_target_is_visible_and_can_return_to_bounded(pair):
    root, a, b = pair
    before = a.read_bytes()
    with sqlite3.connect(b) as db:
        runtime = json.loads(db.execute("SELECT value FROM meta WHERE key='runtime'").fetchone()[0])
        runtime['protocol_version'] = 4
        db.execute("UPDATE meta SET value=? WHERE key='runtime'", (json.dumps(runtime),))
    result = run(root, 'delivery', 'auto', '--budget', 'unlimited', '--to', 'receiver')
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data['budget'] == data['wake_budget'] == data['budget_limit'] == 'unlimited'
    assert data['delivery_state'] == 'active_hooks' and a.read_bytes() == before
    status = json.loads(run(root, 'status', 'receiver', '--json').stdout)[0]
    assert status['remaining'] == status['wake_remaining_effective'] == 'unlimited'
    assert status['warning'] is None
    result = run(root, 'delivery', 'auto', '--budget', '0', '--to', 'receiver')
    assert json.loads(result.stdout)['delivery_state'] == 'paused_budget'
    assert remaining(b) == 0 and a.read_bytes() == before


def test_old_listener_rejects_unlimited_before_any_mutation(pair):
    root, a, b = pair
    from peer_chat import Store
    Store(b).close()  # Match a live listener's already-initialized schema.
    before = a.read_bytes(), b.read_bytes()
    result = run(root, 'delivery', 'auto', '--budget', 'unlimited', '--to', 'receiver')
    assert result.returncode != 0 and 'restart' in result.stderr
    assert (a.read_bytes(), b.read_bytes()) == before
