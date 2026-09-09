"""Reconcile missed Stop/Interrupt hooks from native turn-ending metadata.

Never infer idle from age, process presence, message text, or a missing hook.
Only the latest native terminal event for the last verified hook's exact turn
can end that turn. Interrupted sessions stay paused: Codex does not auto-drain
their queues. No prompt or response text is retained or used as evidence.
"""
from __future__ import annotations

from datetime import datetime
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import time

import peer_platform as pp
from peer_budget import lifecycle_observed
from peer_registry import routing_rejection

TAIL_BYTES = 256 * 1024


def _stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def turn_end_evidence(config, seen):
    """Read a bounded tail of the owning thread's native rollout; fail closed."""
    if not isinstance(config, dict) or not isinstance(seen, dict):
        return None
    try:
        if (seen.get('routing') != 'matched' or seen.get('event') not in ('UserPromptSubmit', 'PostToolUse')
                or not isinstance(seen.get('turn'), str) or not seen['turn']
                or seen.get('owner_identity') != config.get('owner_identity')
                or not config.get('owner_identity')
                or pp.process_identity(config['owner_pid']) != config['owner_identity']):
            return None
        hook_at = seen.get('at')
        if type(hook_at) not in (int, float) or not math.isfinite(hook_at):
            return None
        home = Path(config['codex_home'])
        with sqlite3.connect(f"file:{home / 'state_5.sqlite'}?mode=ro", uri=True, timeout=.2) as db:
            row = db.execute('SELECT rollout_path FROM threads WHERE id=?', (config['thread'],)).fetchone()
        if not row or not isinstance(row[0], str) or not row[0]:
            return None
        path = Path(row[0])
        if routing_rejection({'session_id': config['thread'], 'transcript_path': str(path)},
                             config, check_environment=False):
            return None
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            return None
        with path.open('rb') as stream:
            if _stamp(os.fstat(stream.fileno())) != _stamp(info):
                return None
            offset = max(0, info.st_size - TAIL_BYTES)
            stream.seek(offset)
            raw = stream.read(TAIL_BYTES)
            if _stamp(os.fstat(stream.fileno())) != _stamp(info):
                return None
        if not raw.endswith(b'\n'):
            return None  # An in-flight append can be a newer turn.
        if offset:
            raw = raw.partition(b'\n')[2]  # Never parse a partial first record.
        latest = None
        for line in raw.splitlines():
            record = json.loads(line)
            if not isinstance(record, dict):
                return None
            kind, payload = record.get('type'), record.get('payload')
            if not isinstance(payload, dict):
                return None
            if kind == 'event_msg':
                event = payload.get('type')
                if event in ('task_complete', 'turn_aborted'):
                    latest = record
                elif event != 'token_count':
                    latest = None  # New activity / unsupported event invalidates prior endings.
            else:
                latest = None
        if latest is None or latest['payload'].get('turn_id') != seen['turn']:
            return None
        at = datetime.fromisoformat(latest['timestamp'].replace('Z', '+00:00'))
        if at.tzinfo is None or not hook_at <= at.timestamp() <= time.time():
            return None
        if _stamp(path.stat()) != _stamp(info):
            return None
        return {'source': 'codex_rollout', 'event': latest['payload']['type'],
                'turn': seen['turn'], 'at': at.timestamp(),
                'owner_identity': config['owner_identity'], 'hook_at': hook_at}
    except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, sqlite3.Error):
        return None


def reconcile(store, config):
    """Correct stale phase atomically; preserve budgets, inbox and hooks."""
    previous_phase = store.get('phase')
    if previous_phase not in ('active', 'idle') or store.get('stop', False) or not lifecycle_observed(store):
        return False
    seen = store.get('hook_seen')
    evidence = turn_end_evidence(config, seen)
    if evidence is None:
        return False
    phase = 'interrupted' if evidence['event'] == 'turn_aborted' else 'idle'
    if previous_phase == phase and store.get('lifecycle_reconciled') == evidence:
        return False
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        if (store.get('stop', False) or store.get('phase') != previous_phase
                or store.get('config') != config or store.get('hook_seen') != seen
                or turn_end_evidence(config, seen) != evidence):
            return False
        for key, value in (('phase', phase), ('lifecycle_reconciled', evidence)):
            store.db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, json.dumps(value)))
    return True
