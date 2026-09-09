import pytest

from peer_chat import Store
from peer_budget import EXTERNAL_MARKERS, LIFECYCLE_EVENTS, delivery_state, lifecycle_observed, renew_for_owner_prompt

SEEN = {"at": 1788600000.0, "event": "PostToolUse", "turn": "turn-1", "routing": "matched"}


@pytest.fixture
def exhausted(tmp_path):
    store = Store(tmp_path / "inbox.sqlite")
    for key, value in {"delivery": "auto", "phase": "active", "remaining": 0,
                       "wake_remaining": 0, "budget_limit": 12, "hook_seen": SEEN}.items():
        store.put(key, value)
    yield store
    store.close()


def prompt(text="Continue reviewing the change", turn="owner-turn", event="UserPromptSubmit"):
    return {"hook_event_name": event, "prompt": text, "turn_id": turn}


def test_owner_prompt_renews_once_and_preserves_spending_in_same_turn(exhausted):
    assert delivery_state(exhausted) == "paused_budget"
    assert renew_for_owner_prompt(exhausted, prompt()) is True
    assert exhausted.get("remaining") == exhausted.get("wake_remaining") == 12
    exhausted.put("remaining", 4)
    assert renew_for_owner_prompt(exhausted, prompt()) is False
    assert exhausted.get("remaining") == 4
    assert renew_for_owner_prompt(exhausted, prompt(turn="another-owner-turn")) is True
    assert exhausted.get("remaining") == 12


@pytest.mark.parametrize("marker", EXTERNAL_MARKERS)
def test_peer_queue_input_cannot_renew_its_own_delivery_or_wake_budget(exhausted, marker):
    assert renew_for_owner_prompt(exhausted, prompt("  " + marker + " keep working")) is False
    assert exhausted.get("remaining") == exhausted.get("wake_remaining") == 0
    assert delivery_state(exhausted) == "paused_budget"


@pytest.mark.parametrize("payload", [prompt(event="PostToolUse"), prompt(event="SessionStart"),
                                      prompt(text=None), prompt(text=""), prompt(turn=None), {}])
def test_non_owner_or_unidentified_hook_does_not_reset_allowance(exhausted, payload):
    assert renew_for_owner_prompt(exhausted, payload) is False
    assert exhausted.get("remaining") == 0


def test_explicit_zero_and_legacy_unconfigured_budget_stay_disabled(exhausted):
    exhausted.put("budget_limit", 0)
    assert renew_for_owner_prompt(exhausted, prompt()) is True
    assert exhausted.get("remaining") == exhausted.get("wake_remaining") == 0
    exhausted.db.execute("DELETE FROM meta WHERE key='budget_limit'")
    exhausted.db.commit()
    assert renew_for_owner_prompt(exhausted, prompt(turn="legacy-turn")) is False
    assert exhausted.get("remaining") == 0


def test_idle_wake_budget_pause_is_separate_from_active_delivery(exhausted):
    exhausted.put("remaining", 4)
    exhausted.put("phase", "idle")
    assert delivery_state(exhausted) == "paused_wake_budget"
    exhausted.put("phase", "active")
    assert delivery_state(exhausted) == "active_hooks"
    exhausted.put("delivery", "inbox")
    assert delivery_state(exhausted) == "manual_inbox"


# --------------------------------------------------------------------------
# truthful lifecycle: phase alone is never proof that hooks run
# --------------------------------------------------------------------------


@pytest.mark.parametrize("phase", ["active", "idle"])
@pytest.mark.parametrize("mode", ["auto", "live"])
def test_phase_without_hook_evidence_is_not_ready(exhausted, phase, mode):
    exhausted.put("remaining", 5); exhausted.put("wake_remaining", 5)
    exhausted.put("delivery", mode); exhausted.put("phase", phase)
    with exhausted.db:
        exhausted.db.execute("DELETE FROM meta WHERE key='hook_seen'")
    assert lifecycle_observed(exhausted) is False
    assert delivery_state(exhausted) == "awaiting_lifecycle_hook", "connect/start seeded the phase; no hook ever ran"
    exhausted.put("hook_seen", SEEN)
    assert lifecycle_observed(exhausted) is True
    expected = "active_hooks" if phase == "active" else ("idle_wake_enabled" if mode == "auto" else "idle_live_only")
    assert delivery_state(exhausted) == expected


@pytest.mark.parametrize("seen", [
    None, "matched", {},
    {"at": 1.0, "event": "PostToolUse", "routing": "rejected: transcript_path mismatch"},
    {"at": 1.0, "event": "SubagentStart", "routing": "matched"},
    {"at": 1.0, "event": "", "routing": "matched"},
    {"event": "PostToolUse", "routing": "matched"},
    {"at": "yesterday", "event": "PostToolUse", "routing": "matched"},
    {"at": True, "event": "Stop", "routing": "matched"},
])
def test_lifecycle_observed_requires_matched_real_event_with_timestamp(exhausted, seen):
    exhausted.put("remaining", 5)
    exhausted.put("hook_seen", seen)
    assert lifecycle_observed(exhausted) is False
    assert delivery_state(exhausted) == "awaiting_lifecycle_hook"


@pytest.mark.parametrize("event", LIFECYCLE_EVENTS)
def test_every_lifecycle_event_counts_as_evidence(exhausted, event):
    exhausted.put("remaining", 5)
    exhausted.put("hook_seen", dict(SEEN, event=event))
    assert lifecycle_observed(exhausted) is True
    assert delivery_state(exhausted) == "active_hooks"


def test_precedence_inbox_budget_queue_before_lifecycle(exhausted):
    with exhausted.db:
        exhausted.db.execute("DELETE FROM meta WHERE key='hook_seen'")
    exhausted.put("delivery", "inbox")
    assert delivery_state(exhausted) == "manual_inbox"
    exhausted.put("delivery", "auto"); exhausted.put("remaining", 0)
    assert delivery_state(exhausted) == "paused_budget"
    exhausted.put("remaining", 3); exhausted.put("delivery", "queue")
    assert delivery_state(exhausted) == "after_turn_queue"
    exhausted.put("delivery", "auto")
    assert delivery_state(exhausted) == "awaiting_lifecycle_hook"
    exhausted.put("hook_seen", SEEN); exhausted.put("phase", "unknown")
    assert delivery_state(exhausted) == "awaiting_lifecycle_hook", "evidence without a known phase is still not ready"
    exhausted.put("phase", "idle"); exhausted.put("wake_remaining", 0)
    assert delivery_state(exhausted) == "paused_wake_budget"


@pytest.mark.parametrize('phase,expected', [('active', 'active_hooks'), ('idle', 'idle_wake_enabled')])
def test_unlimited_does_not_need_owner_renewal(exhausted, phase, expected):
    for key in ('remaining', 'wake_remaining', 'budget_limit'):
        exhausted.put(key, 'unlimited')
    exhausted.put('phase', phase)
    assert delivery_state(exhausted) == expected
    assert renew_for_owner_prompt(exhausted, prompt()) is False
    assert exhausted.get('remaining') == exhausted.get('wake_remaining') == 'unlimited'
    exhausted.db.execute("DELETE FROM meta WHERE key='hook_seen'")
    exhausted.db.commit()
    assert delivery_state(exhausted) == 'awaiting_lifecycle_hook'
