"""Bounded delivery windows renewed by owner interaction, never peer wake notices."""
from __future__ import annotations

import json

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
    if store.get("remaining", 0) <= 0:
        return "paused_budget"
    if mode == "queue":
        return "after_turn_queue"
    if not lifecycle_observed(store):
        return "awaiting_lifecycle_hook"
    phase = store.get("phase", "unknown")
    if phase == "unknown":
        return "awaiting_lifecycle_hook"
    if mode == "auto" and phase == "idle" and store.get("wake_remaining", store.get("remaining", 0)) <= 0:
        return "paused_wake_budget"
    return "active_hooks" if phase == "active" else "idle_live_only" if mode == "live" else "idle_wake_enabled"
