"""Observe real database transitions, including older writers; never backfill."""
import json
import os
import sqlite3
import time
import subprocess
import sys
import uuid

import pytest

from peer_chat import Store
from peer_observe import snapshot, render_status
from test_observe import make_bridge, T1


def test_legacy_writer_timing_is_transactional_first_only_and_private(tmp_path):
    path = make_bridge(tmp_path, T1, messages=('consumed', 'received'))
    older = sqlite3.connect(path)  # Writer opened before instrumentation installed.
    store = Store(path)
    assert store.db.execute('SELECT count(*) FROM message_timing').fetchone()[0] == 0
    try:
        older.execute("UPDATE messages SET status='hook_offered' WHERE id='m-1'")
        older.rollback()
        assert store.db.execute('SELECT count(*) FROM message_timing').fetchone()[0] == 0
        before = time.time()
        with older:
            older.execute("UPDATE messages SET status='hook_offered' WHERE id='m-1'")
        first = store.db.execute('SELECT at FROM message_timing').fetchone()[0]
        assert before - .002 <= first <= time.time() + .002
        store.read(ack=True)
        store.read(ack=True)
        rows = [tuple(r) for r in store.db.execute('SELECT * FROM message_timing')]
        assert len(rows) == 2
        assert {r[2] for r in rows} == {'consumed', 'hook_offered'}
        assert all(r[1] == 'm-1' for r in rows)  # Historical consumption stays unknown.
        assert 'SECRET' not in json.dumps(rows)
        row = snapshot(tmp_path, T1)
        assert row['received_total'] == row['acknowledged_total'] == 2
        assert row['oldest_pending_age_s'] is None
        for metric in ('received_to_hook_seconds', 'received_to_ack_seconds'):
            assert row['delivery_timing'][metric]['samples'] == 1
            assert row['delivery_timing'][metric]['max'] >= 0
        assert row['delivery_timing']['received_to_ack_after_observed_hook_seconds']['samples'] == 1
        assert row['delivery_timing']['received_to_ack_without_observed_hook_seconds']['samples'] == 0
    finally:
        older.close()
        store.close()


def test_direct_ack_is_not_counted_as_observed_hook_delivery(tmp_path):
    path = make_bridge(tmp_path, T1, messages=('received',))
    store = Store(path)
    try:
        store.read(ack=True)
        timing = snapshot(tmp_path, T1)['delivery_timing']
        assert timing['received_to_ack_after_observed_hook_seconds']['samples'] == 0
        assert timing['received_to_ack_without_observed_hook_seconds']['samples'] == 1
    finally:
        store.close()


def test_pending_age_and_absent_timing_are_read_only(tmp_path):
    path = make_bridge(tmp_path, T1, messages=('received', 'hook_offered', 'consumed'),
                       extra_meta={'notice:peer': {'result': 'unconfirmed:ValueError', 'at': 123}})
    with sqlite3.connect(path) as db:
        db.execute("UPDATE messages SET created=? WHERE id='m-0'", (time.time() - 7200,))
    before = path.read_bytes()
    row = snapshot(tmp_path, T1)
    assert row['oldest_pending_age_s'] >= 7200
    assert row['pending_age_s_by_status']['received'] >= 7200
    assert row['delivery_timing']['available'] is False
    assert row['delivery_timing']['received_to_ack_seconds'] is None
    assert row['notice_failures'] == [{'peer': 'peer', 'result': 'unconfirmed:ValueError', 'at': 123}]
    assert 'advisory attempt(s) unconfirmed' in row['warning']
    assert 'OLDEST' in render_status([row]) and '2h' in render_status([row])
    assert 'SECRET' not in json.dumps(row)
    assert path.read_bytes() == before
    with sqlite3.connect(path) as db:
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='message_timing'").fetchone()


@pytest.mark.parametrize('prior,current,recovered,expected', [
    ('active_hooks', 'idle_wake_enabled', False, 0),
    ('idle_wake_enabled', 'active_hooks', False, 0),
    ('paused_budget', 'active_hooks', False, 0),
    ('active_hooks', 'idle_wake_enabled', True, 0),
    ('active_hooks', 'paused_budget', False, 0),
    ('active_hooks', 'awaiting_lifecycle_hook', False, 0),
    ('awaiting_lifecycle_hook', 'idle_wake_enabled', False, 0),
])
def test_empty_inbox_never_announces_state_changes_or_recovery(tmp_path, monkeypatch, prior, current, recovered, expected):
    import peer_chat
    import peer_delivery as delivery
    store = Store(tmp_path / 'inbox.sqlite')
    peer = {'kind': 'claude', 'key': 'p', 'identity': 'p', 'pid': 1, 'socket': '/unused'}
    config = {'thread': T1, 'peers': {'p': peer}}
    store.put('runtime', {'pid': os.getpid()})
    store.put('notice:p', {'signature': ['running', prior], 'at': time.time() - 60})
    if recovered:
        store.put('recovery_event', {'id': 'new-recovery'})
    monkeypatch.setattr(delivery, 'view', lambda *_: {'runtime_state': 'running', 'delivery_state': current,
        'reachable': current in ('active_hooks', 'idle_wake_enabled'), 'remaining': 0 if current == 'paused_budget' else 4,
        'warning': 'budget exhausted' if current == 'paused_budget' else None})
    calls = []
    monkeypatch.setattr(peer_chat, 'outbound', lambda *args, **kwargs: calls.append(args))
    try:
        delivery.notify(store, config, tmp_path)
        assert len(calls) == expected
    finally:
        store.close()


def test_startup_grace_drops_transient_warning_but_eventually_warns(tmp_path, monkeypatch):
    import peer_chat
    import peer_delivery as delivery
    store = Store(tmp_path / 'inbox.sqlite')
    peer = {'kind': 'claude', 'key': 'p', 'identity': 'p', 'pid': 1, 'socket': '/unused'}
    config = {'thread': T1, 'peers': {'p': peer}}
    store.put('runtime', {'pid': os.getpid()})
    store.accept('p', {'kind': 'message', 'id': 'fresh', 'body': 'SECRET'})
    current = {'runtime_state': 'running', 'delivery_state': 'awaiting_lifecycle_hook',
               'reachable': False, 'remaining': 12, 'warning': 'waiting for hook'}
    monkeypatch.setattr(delivery, 'view', lambda *_: current)
    calls = []
    monkeypatch.setattr(peer_chat, 'outbound', lambda *args, **kwargs: calls.append(args))
    try:
        delivery.notify(store, config, tmp_path)
        assert not calls and not store.get('notice:p')
        store.set_status('p', 'fresh', 'hook_offered')
        current.update(delivery_state='active_hooks', reachable=True, warning=None)
        delivery.notify(store, config, tmp_path)
        assert not calls  # Startup finished: no stale blocked/ready notice.
        store.accept('p', {'kind': 'message', 'id': 'waiting', 'body': 'SECRET'})
        current.update(delivery_state='awaiting_lifecycle_hook', reachable=False, warning='waiting for hook')
        with store.db:
            store.db.execute("UPDATE messages SET created=? WHERE id='waiting'", (time.time() - 21,))
        delivery.notify(store, config, tmp_path)
        assert len(calls) == 1 and 'waiting' in calls[0][1]
        record = store.get('notice_message:p:waiting')
        assert record['state'] == 'awaiting_lifecycle_hook' and record['remaining'] == 12
        assert 'SECRET' not in json.dumps(record)
    finally:
        store.close()


def test_message_status_is_bounded_exact_and_never_reads_or_acknowledges(tmp_path):
    from peer_observe import message_status
    path = make_bridge(tmp_path, T1, messages=('received',) * 22)
    old_id = str(uuid.uuid4())
    with sqlite3.connect(path) as db:
        db.execute("UPDATE messages SET id=?,created=? WHERE id='m-0'", (old_id, time.time() - 1000))
    before = path.read_bytes()
    result = message_status(tmp_path, T1)
    assert len(result['messages']) == 20 and result['more']
    assert all(r['id'] != old_id for r in result['messages'])
    exact = message_status(tmp_path, T1, old_id)
    assert len(exact['messages']) == 1 and not exact['more']
    row = exact['messages'][0]
    assert row['status'] == 'received' and row['acknowledged'] is False
    assert row['hook_offered_at'] is row['acknowledged_at'] is row['last_wake_state'] is None
    assert 'SECRET' not in json.dumps(result) + json.dumps(exact)
    assert path.read_bytes() == before


@pytest.mark.parametrize('target', ['positional', 'global_thread', 'status_thread'])
def test_message_status_cli_routes_recipient_read_only(tmp_path, target):
    path = make_bridge(tmp_path, T1, messages=('received',))
    mid = str(uuid.uuid4())
    with sqlite3.connect(path) as db:
        db.execute('UPDATE messages SET id=?', (mid,))
    before = path.read_bytes()
    args = {'positional': ['status', T1], 'global_thread': ['--thread', T1, 'status'],
            'status_thread': ['status', '--thread', T1]}[target]
    output = subprocess.run([sys.executable, '-m', 'peer_chat', '--state-root', str(tmp_path),
        *args, '--message-id', mid], capture_output=True, text=True, check=True).stdout
    row = json.loads(output)[0]['message_status']['messages'][0]
    assert row['id'] == mid and row['status'] == 'received'
    assert 'SECRET' not in output
    assert path.read_bytes() == before


def test_blocked_notice_is_per_message_and_sender_not_state_change(tmp_path, monkeypatch):
    import peer_chat
    import peer_delivery as delivery
    store = Store(tmp_path / 'inbox.sqlite')
    config = {'thread': T1, 'peers': {key: {
        'kind': 'claude', 'key': key, 'identity': key, 'pid': 1, 'socket': '/unused'
    } for key in ('p', 'other')}}
    store.put('runtime', {'pid': os.getpid()})
    store.accept('p', {'kind': 'message', 'id': 'blocked', 'body': 'SECRET'})
    current = {'runtime_state': 'running', 'delivery_state': 'paused_budget',
               'reachable': False, 'remaining': 0, 'warning': 'budget exhausted'}
    monkeypatch.setattr(delivery, 'view', lambda *_: current)
    calls = []
    monkeypatch.setattr(peer_chat, 'outbound', lambda *args, **kwargs: calls.append(args))
    try:
        delivery.notify(store, config, tmp_path)
        assert len(calls) == 1 and 'blocked' in calls[0][1]
        assert not store.get('notice:other')
        # Alternating rejected/accepted hook status used to announce repeatedly.
        for i in range(5):
            for state in ('active_hooks', 'awaiting_lifecycle_hook', 'idle_wake_enabled', 'paused_budget'):
                current.update(delivery_state=state, reachable=state in ('active_hooks', 'idle_wake_enabled'))
                store.put('recovery_event', {'id': str(i)})
                delivery.notify(store, config, tmp_path)
        assert len(calls) == 1
        store.accept('p', {'kind': 'message', 'id': 'second', 'body': 'SECRET'})
        delivery.notify(store, config, tmp_path)
        assert len(calls) == 2 and 'second' in calls[1][1]
        assert 'SECRET' not in str(calls)
        store.set_status('p', 'blocked', 'consumed')
        store.set_status('p', 'second', 'hook_offered')
        current.update(delivery_state='idle_wake_enabled', reachable=True)
        delivery.notify(store, config, tmp_path)
        assert len(calls) == 2
    finally:
        store.close()


def test_rejected_hook_diagnostic_does_not_hide_accepted_lifecycle(tmp_path):
    rejected = {'at': time.time(), 'event': 'PostToolUse', 'turn': 'child',
                'routing': 'rejected: subagent marker', 'owner_identity': 'PRIVATE'}
    path = make_bridge(tmp_path, T1, extra_meta={'hook_rejected': rejected})
    before = path.read_bytes()
    row = snapshot(tmp_path, T1)
    assert row['delivery_state'] == 'active_hooks' and row['reachable']
    assert row['hook_seen']['routing'] == 'matched'
    assert row['hook_rejected']['routing'] == 'rejected: subagent marker'
    assert 'PRIVATE' not in json.dumps(row)
    assert path.read_bytes() == before


def test_held_message_warns_once_even_when_bridge_is_ready(tmp_path, monkeypatch):
    import peer_chat
    import peer_delivery as delivery
    store = Store(tmp_path / 'inbox.sqlite')
    config = {'thread': T1, 'peers': {'p': {'kind': 'claude', 'key': 'p', 'identity': 'p', 'pid': 1, 'socket': '/unused'}}}
    store.put('runtime', {'pid': os.getpid()})
    store.accept('p', {'kind': 'message', 'id': 'held-id', 'body': 'SECRET'})
    store.set_status('p', 'held-id', 'held', 'Hop limit reached')
    monkeypatch.setattr(delivery, 'view', lambda *_: {'runtime_state': 'running', 'delivery_state': 'idle_wake_enabled', 'reachable': True, 'remaining': 12})
    calls = []
    monkeypatch.setattr(peer_chat, 'outbound', lambda *args, **kwargs: calls.append(args))
    try:
        delivery.notify(store, config, tmp_path)
        delivery.notify(store, config, tmp_path)
        assert len(calls) == 1 and 'held-id' in calls[0][1] and '"state": "held"' in calls[0][1]
        assert 'SECRET' not in calls[0][1]
        assert store.get('notice_message:p:held-id')['state'] == 'held'
    finally:
        store.close()


def test_held_reason_is_visible_without_exposing_arbitrary_detail(tmp_path):
    from peer_observe import message_status
    path = make_bridge(tmp_path, T1, messages=('held', 'held'))
    with sqlite3.connect(path) as db:
        db.execute("UPDATE messages SET detail='Hop limit reached' WHERE id='m-0'")
        db.execute("UPDATE messages SET detail='SECRET' WHERE id='m-1'")
    before = path.read_bytes()
    rows = message_status(tmp_path, T1)['messages']
    assert {r['hold_reason'] for r in rows} == {'hop_limit', 'held'}
    assert 'held by a routing guard' in snapshot(tmp_path, T1)['warning']
    assert 'SECRET' not in json.dumps(rows) and path.read_bytes() == before
