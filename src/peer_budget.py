"""Continuous delivery by default, with optional owner-configured windows."""
from __future__ import annotations

import json

UNLIMITED = "unlimited"


def parse_budget(value):
    if value == UNLIMITED:
        return UNLIMITED
    try:
        return max(0, min(int(value), 50))
    except (ValueError, TypeError):
        raise ValueError("budget must be an integer or 'unlimited'") from None


def available(value):
    return value == UNLIMITED or (type(value) is int and value > 0)


def spend(value):
    return UNLIMITED if value == UNLIMITED else max(0, value - 1)


def require_compatible_listener(store, thread, budget):
    """Do not write new counter values while an older dispatcher can read them."""
    if budget != UNLIMITED:
        return
    from peer_platform import process_identity
    runtime = store.get('runtime') or {}
    if (runtime.get('pid') and runtime.get('protocol_version', 1) < 4
            and process_identity(runtime['pid']) == runtime.get('identity')):
        raise ValueError(f"Listener predates unlimited delivery; run peer-chat --thread {thread} restart, then repeat the budget change")

# Both automatic queue formats used by this package. They reach Codex's user
# input hook as external data, so UserPromptSubmit alone is not owner evidence.
EXTERNAL_MARKERS = (
    "PEER CHAT DELIVERY NOTICE",
    "EXTERNAL PEER INBOX NOTICE",
    "EXTERNAL AGENT MESSAGE",
    "<cross-session-message",
    "[Peer bridge transport self-test;",
    "Peer Chat setup verification continuation,",
)


def renew_for_owner_prompt(store, payload):
    if payload.get("hook_event_name") != "UserPromptSubmit":
        return False
    prompt, turn = payload.get("prompt"), payload.get("turn_id")
    if not isinstance(prompt, str) or not prompt.strip() or not isinstance(turn, str) or not turn:
        return False
    if any(marker in prompt for marker in EXTERNAL_MARKERS):
        return False
    with store.db:
        store.db.execute("BEGIN IMMEDIATE")
        limit = store.get("budget_limit")
        # Legacy stores have no declared window size. Preserve them until an
        # explicit budget is configured; never infer a new allowance from zero.
        if type(limit) is not int or not 0 <= limit <= 50 or store.get("budget_owner_turn") == turn:
            return False
        for key, value in (("remaining", limit), ("wake_remaining", limit), ("budget_owner_turn", turn)):
            store.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))
    return True


LIFECYCLE_EVENTS = ("SessionStart", "UserPromptSubmit", "PostToolUse", "Stop")


def lifecycle_observed(store):
    """The only proof that this session's hooks run: a hook_seen record written by
    the hook itself with routing "matched" and a real lifecycle event. A phase set
    by connect/start from the live caller is process-activity evidence, not proof
    that hooks are loaded, so it never satisfies this predicate."""
    seen = store.get("hook_seen")
    return (isinstance(seen, dict) and seen.get("routing") == "matched"
            and seen.get("event") in LIFECYCLE_EVENTS and isinstance(seen.get("at"), (int, float))
            and not isinstance(seen.get("at"), bool))


hook_evidence = lifecycle_observed


def delivery_state(store):
    mode = store.get("delivery", "inbox")
    if mode == "inbox":
        return "manual_inbox"
    if not available(store.get("remaining", 0)):
        return "paused_budget"
    if mode == "queue":
        return "after_turn_queue"
    if not lifecycle_observed(store):
        return "awaiting_lifecycle_hook"
    phase = store.get("phase", "unknown")
    if phase == "unknown":
        return "awaiting_lifecycle_hook"
    if mode == "auto" and phase == "idle" and not available(store.get("wake_remaining", store.get("remaining", 0))):
        return "paused_wake_budget"
    return "active_hooks" if phase == "active" else "idle_live_only" if mode == "live" else "idle_wake_enabled"


def configure_delivery(store, thread, mode, budget, state_root):
    """Change one incoming window atomically and identify still-blocked recipients."""
    require_compatible_listener(store, thread, budget)
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        values = [('delivery', mode)]
        if budget is not None:
            limit = parse_budget(budget)
            values += [(key, limit) for key in ('remaining', 'wake_remaining', 'budget_limit')]
        for key, value in values:
            store.db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, json.dumps(value)))
        result = {'thread': thread, 'scope': 'incoming_only', 'delivery': mode,
                  'budget': store.get('remaining'), 'wake_budget': store.get('wake_remaining'),
                  'budget_limit': store.get('budget_limit'), 'delivery_state': delivery_state(store)}
        config = store.get('config') or {}
    from peer_peers import all_peers
    from peer_observe import snapshot
    current = snapshot(state_root, thread)
    if current:
        result.update({k: current[k] for k in ('name', 'runtime_state', 'delivery_state')})
    blocked = []
    for peer in all_peers(config).values():
        if peer.get('kind') != 'codex':
            continue
        row = snapshot(peer.get('state_root', state_root), peer['thread'])
        if row and row['delivery_state'] in ('paused_budget', 'paused_wake_budget'):
            blocked.append({k: row[k] for k in ('thread', 'name', 'delivery_state', 'remaining', 'pending_count')})
    result['blocked_recipients'] = blocked
    result['note'] = "Changed only this thread's incoming window. Other sessions have separate receiving allowances."
    if blocked:
        result['warning'] = ('Connected recipients still have exhausted allowances. '
            'With owner authorization, target the intended recipient using delivery MODE --budget N --to NAME; '
            "then check that recipient's status. Waiting on this inbox does not renew another session.")
    return result
