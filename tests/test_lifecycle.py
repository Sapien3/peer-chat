"""Missed Stop/Interrupt hooks must not strand messages behind stale active state."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import uuid

import pytest

import peer_chat
import peer_lifecycle as lifecycle
import peer_platform as pp
from peer_observe import snapshot


@pytest.fixture
def ended(tmp_path):
    home = tmp_path / 'codex'; home.mkdir()
    thread, turn = str(uuid.uuid4()), str(uuid.uuid4())
    rollout = home / 'rollout.jsonl'
    with sqlite3.connect(home / 'state_5.sqlite') as db:
        db.execute('CREATE TABLE threads(id TEXT, rollout_path TEXT, archived INT)')
        db.execute('INSERT INTO threads VALUES (?,?,0)', (thread, str(rollout)))
    store = peer_chat.Store(tmp_path / 'state' / thread / 'inbox.sqlite')
    config = {'thread': thread, 'codex_home': str(home), 'owner_pid': os.getpid(),
              'owner_identity': pp.process_identity(os.getpid()), 'codex_bin': 'fixture-codex',
              'peer': {'identity': 'peer', 'pid': 1, 'name': 'Reviewer', 'socket': '/fixture.sock'},
              'socket': '/missing-fixture.sock', 'supervision': True}
    at = time.time() - 10
    seen = {'event': 'UserPromptSubmit', 'routing': 'matched', 'turn': turn, 'at': at,
            'owner_identity': config['owner_identity']}
    for key, value in {'config': config, 'hook_seen': seen, 'phase': 'active', 'delivery': 'auto',
                       'remaining': 40, 'wake_remaining': 40, 'budget_limit': 40,
                       'runtime': {'pid': os.getpid(), 'identity': config['owner_identity'], 'protocol_version': 6}}.items():
        store.put(key, value)
    mid = str(uuid.uuid4())
    store.accept('peer', {'id': mid, 'kind': 'message', 'body': 'UNTRUSTED PEER BODY', 'hops': []})
    event = {'timestamp': datetime.fromtimestamp(at + 2, timezone.utc).isoformat(),
             'type': 'event_msg', 'payload': {'type': 'turn_aborted', 'turn_id': turn, 'reason': 'interrupted'}}
    rollout.write_text(json.dumps(event) + '\n')
    yield store, config, seen, rollout, event, mid
    store.close()


def append(path, record):
    with path.open('a') as stream:
        stream.write(json.dumps(record) + '\n')


def test_exact_native_completion_restores_idle_and_wakes_once(ended, monkeypatch):
    store, config, seen, rollout, event, mid = ended
    event['payload']['type'] = 'task_complete'
    rollout.write_text(json.dumps(event) + '\n')
    before_messages = store.read()
    calls = []
    monkeypatch.setattr(peer_chat.subprocess, 'run', lambda args, **kw:
        calls.append(args) or subprocess.CompletedProcess(args, 0, '', ''))
    assert lifecycle.reconcile(store, config)
    assert store.get('phase') == 'idle'
    assert store.get('hook_seen') == seen  # No synthetic hook or renewal.
    assert store.get('remaining') == store.get('wake_remaining') == 40
    assert store.read() == before_messages
    peer_chat.dispatch(store, config); peer_chat.dispatch(store, config)
    assert len(calls) == 1 and mid in calls[0][-1]
    assert 'UNTRUSTED PEER BODY' not in calls[0][-1]
    assert store.get('remaining') == 40 and store.get('wake_remaining') == 39
    assert store.read()[0]['status'] == 'received'
    assert not lifecycle.reconcile(store, config)


@pytest.mark.parametrize('change', ['turn', 'old_timestamp', 'future_timestamp', 'naive_timestamp',
                                    'quoted', 'partial', 'malformed', 'new_turn', 'new_context', 'later_response',
                                    'wrong_owner', 'missing_owner_proof', 'missing_hook', 'archived', 'child',
                                    'missing_rollout', 'symlink', 'unknown_phase', 'stopped'])
def test_uncertain_stale_or_untrusted_evidence_never_ends_a_turn(ended, change):
    store, config, seen, rollout, event, mid = ended
    if change == 'turn': event['payload']['turn_id'] = str(uuid.uuid4())
    elif change == 'old_timestamp': event['timestamp'] = datetime.fromtimestamp(seen['at'] - 1, timezone.utc).isoformat()
    elif change == 'future_timestamp': event['timestamp'] = datetime.fromtimestamp(time.time() + 60, timezone.utc).isoformat()
    elif change == 'naive_timestamp': event['timestamp'] = '2026-01-01T00:00:00'
    elif change == 'quoted': event = {'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': json.dumps(event)}}
    elif change == 'wrong_owner': config['owner_identity'] = 'different-owner'; store.put('config', config)
    elif change == 'missing_owner_proof': seen.pop('owner_identity'); store.put('hook_seen', seen)
    elif change == 'missing_hook': store.put('hook_seen', None)
    elif change == 'unknown_phase': store.put('phase', 'unknown')
    elif change == 'stopped': store.put('stop', True)
    elif change == 'archived':
        with sqlite3.connect(Path(config['codex_home']) / 'state_5.sqlite') as db: db.execute('UPDATE threads SET archived=1')
    elif change == 'child':
        with sqlite3.connect(Path(config['codex_home']) / 'state_5.sqlite') as db:
            db.execute('CREATE TABLE thread_spawn_edges(child_thread_id TEXT)')
            db.execute('INSERT INTO thread_spawn_edges VALUES (?)', (config['thread'],))
    rollout.write_text(json.dumps(event) + '\n')
    if change == 'partial':
        with rollout.open('a') as f: f.write('{"type":"event_msg"')
    elif change == 'malformed':
        with rollout.open('a') as f: f.write('not-json\n')
    elif change == 'new_turn': append(rollout, {'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': str(uuid.uuid4())}})
    elif change == 'new_context': append(rollout, {'type': 'turn_context', 'payload': {'turn_id': str(uuid.uuid4())}})
    elif change == 'later_response': append(rollout, {'type': 'response_item', 'payload': {'type': 'message', 'role': 'assistant'}})
    elif change == 'missing_rollout': rollout.unlink()
    elif change == 'symlink':
        target = rollout.with_suffix('.target'); rollout.rename(target); rollout.symlink_to(target)
    before = list(store.db.execute('SELECT key,value FROM meta ORDER BY key'))
    assert not lifecycle.reconcile(store, config)
    assert list(store.db.execute('SELECT key,value FROM meta ORDER BY key')) == before
    assert store.read()[0]['status'] == 'received'


def test_long_active_turn_never_becomes_idle_from_age(ended):
    store, config, seen, rollout, event, _ = ended
    rollout.write_text(json.dumps({'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': seen['turn']}}) + '\n')
    seen['at'] -= 86400; store.put('hook_seen', seen)
    assert not lifecycle.reconcile(store, config)
    assert store.get('phase') == 'active'


@pytest.mark.parametrize('race', ['hook', 'append', 'owner', 'stop'])
def test_race_after_observation_cannot_overwrite_newer_state(ended, monkeypatch, race):
    store, config, seen, rollout, event, _ = ended
    original = lifecycle.turn_end_evidence
    def observe(*args):
        result = original(*args)
        if race == 'hook': store.put('hook_seen', {**seen, 'turn': 'new-turn'})
        elif race == 'append': append(rollout, {'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': 'new-turn'}})
        elif race == 'owner': store.put('config', {**config, 'owner_identity': 'replacement'})
        elif race == 'stop': store.put('stop', True)
        monkeypatch.setattr(lifecycle, 'turn_end_evidence', original)
        return result
    monkeypatch.setattr(lifecycle, 'turn_end_evidence', observe)
    assert not lifecycle.reconcile(store, config)
    assert store.get('phase') == 'active'
    assert store.get('lifecycle_reconciled') is None


def test_tail_is_bounded_and_only_terminal_metadata_is_retained(ended):
    store, config, seen, rollout, event, _ = ended
    prefix = json.dumps({'type': 'response_item', 'payload': {'content': 'PRIVATE' * lifecycle.TAIL_BYTES}})
    rollout.write_text(prefix + '\n' + json.dumps(event) + '\n')
    assert lifecycle.reconcile(store, config)
    assert 'PRIVATE' not in json.dumps(store.get('lifecycle_reconciled'))


def test_status_reports_native_idle_without_mutating_inbox(ended, monkeypatch):
    store, config, seen, rollout, event, _ = ended
    import peer_observe
    monkeypatch.setattr(peer_observe, '_socket_ok', lambda _: True)
    before = list(store.db.execute('SELECT key,value FROM meta ORDER BY key'))
    row = snapshot(Path(config['codex_home']).parent / 'state', config['thread'])
    assert row['phase'] == 'interrupted' and row['delivery_state'] == 'paused_interrupted'
    assert not row['reachable'] and 'explicitly resume' in row['warning']
    assert row['phase_evidence']['event'] == 'turn_aborted'
    assert row['hook_seen']['event'] == 'UserPromptSubmit'
    assert list(store.db.execute('SELECT key,value FROM meta ORDER BY key')) == before
    assert store.read()[0]['status'] == 'received'


def test_old_listener_is_not_advertised_as_ready_to_recover(ended, monkeypatch):
    store, config, seen, rollout, event, _ = ended
    import peer_observe
    monkeypatch.setattr(peer_observe, '_socket_ok', lambda _: True)
    event['payload']['type'] = 'task_complete'
    rollout.write_text(json.dumps(event) + '\n')
    store.put('runtime', {**store.get('runtime'), 'protocol_version': 4})
    row = snapshot(Path(config['codex_home']).parent / 'state', config['thread'])
    assert row['delivery_state'] == 'awaiting_listener_upgrade' and not row['reachable']
    assert 'watch start' in row['warning']


def test_old_hook_warning_requires_an_actually_waiting_message(ended, monkeypatch):
    store, config, seen, rollout, event, _ = ended
    import peer_observe
    monkeypatch.setattr(peer_observe, '_socket_ok', lambda _: True)
    rollout.write_text(json.dumps({'type': 'event_msg', 'payload': {'type': 'task_started'}}) + '\n')
    seen['at'] = time.time() - 600; store.put('hook_seen', seen)
    root = Path(config['codex_home']).parent / 'state'
    assert snapshot(root, config['thread'])['warning'] is None
    with store.db:
        store.db.execute('UPDATE messages SET created=?', (time.time() - 120,))
    row = snapshot(root, config['thread'])
    assert row['phase'] == 'active'
    assert 'does not prove' in row['warning']


@pytest.mark.parametrize('mode', ['auto', 'live', 'queue'])
def test_interrupt_is_paused_without_wake_or_budget_renewal(ended, monkeypatch, mode):
    from peer_budget import delivery_state
    store, config, seen, rollout, event, mid = ended
    store.put('delivery', mode)
    monkeypatch.setattr(peer_chat.subprocess, 'run', lambda *a, **k: pytest.fail('interrupted Codex cannot auto-wake'))
    assert lifecycle.reconcile(store, config)
    assert store.get('phase') == 'interrupted' and delivery_state(store) == 'paused_interrupted'
    peer_chat.dispatch(store, config)
    assert store.get('wake_pending') is None
    assert store.get('remaining') == store.get('wake_remaining') == 40
    assert store.read()[0]['status'] == 'received'


def test_explicit_owner_resume_delivers_retained_message(ended, monkeypatch):
    import peer_hooks
    store, config, seen, rollout, event, mid = ended
    assert lifecycle.reconcile(store, config)
    monkeypatch.setenv('CODEX_HOME', config['codex_home'])
    monkeypatch.setenv('CODEX_THREAD_ID', config['thread'])
    monkeypatch.setattr(pp, 'find_owner_pid', lambda *_: os.getpid())
    payload = {'session_id': config['thread'], 'transcript_path': str(rollout),
               'hook_event_name': 'UserPromptSubmit', 'turn_id': str(uuid.uuid4()), 'prompt': 'Resume'}
    result = peer_hooks.deliver(payload, Path(config['codex_home']).parent / 'state')
    assert mid in result['hookSpecificOutput']['additionalContext']
    assert store.get('phase') == 'active' and store.read()[0]['status'] == 'hook_offered'
    assert not lifecycle.reconcile(store, store.get('config'))


def test_old_idle_reconciliation_is_corrected_to_interrupted(ended):
    store, config, seen, rollout, event, _ = ended
    store.put('phase', 'idle')
    store.put('lifecycle_reconciled', lifecycle.turn_end_evidence(config, seen))
    assert lifecycle.reconcile(store, config)
    assert store.get('phase') == 'interrupted'
