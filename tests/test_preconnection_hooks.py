"""A greeting before connect must not require a second owner prompt afterward."""
import json
import sys
import time

import pytest

from peer_registry import seed_lifecycle
from test_codex_mesh import mesh, send

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="real /proc writer-lock harness")


def hook(owner, event, **changes):
    payload = {'session_id': owner.thread, 'transcript_path': str(owner.home.parent / (owner.thread + '.jsonl')),
               'hook_event_name': event, 'turn_id': 'greeting', **changes}
    code = 'import json,sys; from peer_hooks import deliver; print(json.dumps(deliver(json.loads(sys.argv[1]), sys.argv[2])))'
    owner.proc.stdin.write(json.dumps([sys.executable, '-c', code, json.dumps(payload), str(owner.state)]) + '\n')
    owner.proc.stdin.flush()
    result = json.loads(owner.proc.stdout.readline())
    assert result['code'] == 0, result.get('stderr')
    return json.loads(result['stdout'])


def test_greeting_before_connect_retains_verified_idle_and_delivers(mesh):
    root, (a, b, _) = mesh
    hook(b, 'UserPromptSubmit', prompt='Hello')
    hook(b, 'Stop')
    assert not (b.state / b.thread / 'inbox.sqlite').exists()
    evidence_path = b.state / 'registry/hooks' / (b.thread + '.json')
    before = json.loads(evidence_path.read_text())
    assert before['event'] == 'Stop' and before['routing'] == 'matched'
    # Renaming and rejected child events cannot erase the actual hook evidence.
    b.command('register', '--name', 'B')
    hook(b, 'PostToolUse', agent_id='child')
    assert json.loads(evidence_path.read_text()) == before
    connected = a.command('connect', 'B')
    assert connected['recipient_phase'] == 'idle'
    assert connected['recipient_delivery_state'] == 'idle_wake_enabled'
    assert connected['destination']['reachable']
    store = b.store()
    try:
        assert store.get('hook_seen')['at'] == before['at']
        assert store.get('hook_seen')['source'] == 'preconnection_hook'
        assert store.get('remaining') == store.get('wake_remaining') == 'unlimited'
    finally:
        store.close()
    # Active boundaries deliver using the existing hook path, no owner prompt.
    hook(b, 'PostToolUse')
    mid = send(a, root, 'Review this bounded change', '--to', 'B')
    context = hook(b, 'PostToolUse')['hookSpecificOutput']['additionalContext']
    assert mid in context
    assert hook(b, 'PostToolUse') == {}


@pytest.mark.parametrize('change', [
    {'owner_identity': 'old-owner'}, {'owner_pid': -1}, {'codex_home': '/different-home'},
    {'thread': '22222222-2222-4222-8222-222222222222'},
    {'transcript_path': '/other-thread'}, {'event': 'unknown'},
    {'routing': 'rejected: subagent marker'}, {'at': float('nan')},
    {'at': time.time() + 10000},
])
def test_old_or_unverified_evidence_cannot_make_new_bridge_ready(mesh, change):
    _, (a, b, _) = mesh
    hook(b, 'Stop')
    path = b.state / 'registry/hooks' / (b.thread + '.json')
    record = json.loads(path.read_text()); record.update(change);path.write_text(json.dumps(record))
    result = a.command('connect', 'B')
    assert result['recipient_delivery_state'] == 'awaiting_lifecycle_hook'
    assert not result['destination']['reachable']


def test_newer_inbox_hook_wins_over_preconnection_record(mesh):
    _, (a, b, _) = mesh
    hook(b, 'Stop')
    a.command('connect', 'B')
    store = b.store()
    try:
        seen = dict(store.get('hook_seen'), event='PostToolUse', at=time.time() + 1)
        store.put('hook_seen', seen);store.put('phase', 'active')
        store.put('remaining', 0);store.put('wake_remaining', 0)
        assert not seed_lifecycle(store, b.state)
        assert store.get('hook_seen') == seen and store.get('phase') == 'active'
        assert store.get('remaining') == store.get('wake_remaining') == 0
    finally:
        store.close()
