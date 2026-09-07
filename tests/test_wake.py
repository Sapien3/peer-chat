"""Tests for peer_wake.py: bounded, metadata-only wake lifecycle evidence."""

import json
import sqlite3
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import peer_wake as pw  # noqa: E402

SECRET = "SECRET-BODY-MUST-NOT-PERSIST"


class MiniStore:
    """The duck-type peer_wake needs: a sqlite connection with a meta table."""

    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);")

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))

    def close(self):
        self.db.close()


@pytest.fixture
def store(tmp_path):
    s = MiniStore(tmp_path / "inbox.sqlite")
    yield s
    s.close()


def dump(tmp_path):
    conn = sqlite3.connect(f"file:{tmp_path / 'inbox.sqlite'}?mode=ro", uri=True)
    try:
        return "\n".join(repr(r) for r in conn.execute("SELECT * FROM meta"))
    finally:
        conn.close()


def prompt_hook(prompt, turn="turn-1"):
    return {"hook_event_name": "UserPromptSubmit", "prompt": prompt, "turn_id": turn, "session_id": "s", "tool_input": {"x": SECRET}}


def tool_hook(turn="turn-1"):
    return {"hook_event_name": "PostToolUse", "turn_id": turn, "tool_input": {"command": SECRET}, "tool_response": SECRET}


def notice(mid):
    return pw.notice_text(mid)


def claim(store, mid, status="queued", queue_id=None):
    """A wake always begins with the producer's writing claim, then its completion."""
    pw.record_wake(store, {"id": mid, "status": "writing"})
    if status != "writing":
        marker = {"id": mid, "status": status}
        if queue_id:
            marker["queue_id"] = queue_id
        return pw.record_wake(store, marker)
    return pw.wake_summary(store)["last"]


# --------------------------------------------------------------------------
# happy path
# --------------------------------------------------------------------------


def test_full_lifecycle_writing_queued_activated(store, tmp_path):
    mid = str(uuid.uuid4())
    rec = pw.record_wake(store, {"id": mid, "status": "writing", "at": time.time()})
    assert rec["state"] == "writing" and rec["id"] == mid and rec["queue_id"] is None
    assert store.get("wake_pending") == {"id": mid, "status": "writing", "at": rec["at"]}
    rec = pw.record_wake(store, {"id": mid, "status": "queued", "queue_id": "q-1"})
    assert rec["state"] == "queued" and rec["queue_id"] == "q-1"
    assert store.get("wake_pending")["status"] == "queued" and store.get("wake_pending")["queue_id"] == "q-1"
    assert pw.wake_summary(store)["counts"] == {"activated": 0, "raced": 0, "cancelled": 0, "failed": 0, "uncertain": 0}
    rec = pw.active_hook(store, prompt_hook(notice(mid), turn="turn-9"))
    assert rec["state"] == "activated" and rec["event"] == "UserPromptSubmit" and rec["turn"] == "turn-9"
    assert rec["queue_id"] == "q-1"
    assert store.get("wake_pending") is None
    summary = pw.wake_summary(store)
    assert summary["pending"] is None and summary["last"]["state"] == "activated"
    assert summary["counts"]["activated"] == 1 and sum(summary["counts"].values()) == 1
    assert SECRET not in dump(tmp_path), "no prompt or tool text is ever persisted"


def test_activation_requires_exact_canonical_notice(store):
    # A wake id is a message id and wakes once, so every scenario uses a fresh id.
    mid = str(uuid.uuid4())
    claim(store, mid, "queued", "q")
    assert pw.active_hook(store, prompt_hook("please continue"))["state"] == "cancelled"

    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    claim(store, a, "queued")
    rec = pw.active_hook(store, prompt_hook(notice(b)))
    assert rec["id"] == a and rec["state"] == "cancelled", "a notice for a different id is not proof"

    c = str(uuid.uuid4())
    claim(store, c, "queued")
    assert pw.active_hook(store, prompt_hook(notice(c).upper()))["state"] == "cancelled", "match is exact"

    d = str(uuid.uuid4())
    claim(store, d, "queued")
    assert pw.active_hook(store, prompt_hook(f"what is message {d}?"))["state"] == "cancelled", "an id mentioned in a question is not proof"

    e = str(uuid.uuid4())
    claim(store, e, "queued")
    assert pw.active_hook(store, prompt_hook(notice(e) + " plus owner text"))["state"] == "cancelled", "notice plus extra text is not the queued notice"

    f = str(uuid.uuid4())
    claim(store, f, "queued")
    assert pw.active_hook(store, prompt_hook("  " + notice(f) + "\n"))["state"] == "activated", "surrounding whitespace is tolerated"

    g = str(uuid.uuid4())
    claim(store, g, "queued")
    assert pw.active_hook(store, prompt_hook(notice(g)))["state"] == "activated"
    counts = pw.wake_summary(store)["counts"]
    assert counts == {"activated": 2, "raced": 0, "cancelled": 5, "failed": 0, "uncertain": 0}


def test_same_id_cannot_wake_twice(store):
    mid = str(uuid.uuid4())
    claim(store, mid, "queued")
    assert pw.active_hook(store, tool_hook())["state"] == "cancelled"
    assert claim(store, mid, "queued")["state"] == "cancelled", "re-claiming a decided id is refused"
    assert store.get("wake_pending") is None


def test_post_tool_use_never_activates(store):
    mid = str(uuid.uuid4())
    claim(store, mid, "queued")
    rec = pw.active_hook(store, tool_hook(turn="t-2"))
    assert rec["state"] == "cancelled" and rec["event"] == "PostToolUse" and rec["turn"] == "t-2"
    assert store.get("wake_pending") is None
    assert pw.wake_summary(store)["counts"]["cancelled"] == 1


# --------------------------------------------------------------------------
# producer/consumer race and monotonic rule
# --------------------------------------------------------------------------


def test_active_hook_before_queue_rpc_returns_is_raced_and_completion_cannot_regress(store):
    mid = str(uuid.uuid4())
    pw.record_wake(store, {"id": mid, "status": "writing"})
    raced = pw.active_hook(store, prompt_hook("unrelated owner prompt"))
    assert raced["state"] == "raced" and store.get("wake_pending") is None
    late = pw.record_wake(store, {"id": mid, "status": "queued", "queue_id": "q-late"})
    assert late["state"] == "raced", "producer completion must not overwrite the hook's decision"
    assert late["queue_id"] == "q-late", "but the queue id is backfilled"
    assert late["at"] == raced["at"]
    assert store.get("wake_pending") is None
    counts = pw.wake_summary(store)["counts"]
    assert counts == {"activated": 0, "raced": 1, "cancelled": 0, "failed": 0, "uncertain": 0}


def test_activated_result_survives_late_producer_and_repeated_hooks(store):
    mid = str(uuid.uuid4())
    pw.record_wake(store, {"id": mid, "status": "writing"})
    first = pw.active_hook(store, prompt_hook(notice(mid)))
    assert first["state"] == "activated"
    assert pw.record_wake(store, {"id": mid, "status": "queued", "queue_id": "q-1"})["state"] == "activated"
    assert pw.record_wake(store, {"id": mid, "status": "failed"})["state"] == "activated"
    assert pw.active_hook(store, prompt_hook(notice(mid))) is None, "nothing pending: repeated hooks are no-ops"
    assert pw.active_hook(store, tool_hook()) is None
    counts = pw.wake_summary(store)["counts"]
    assert counts["activated"] == 1 and sum(counts.values()) == 1
    last = pw.wake_summary(store)["last"]
    assert last["state"] == "activated" and last["queue_id"] == "q-1"


def test_states_are_monotonic_and_timestamps_never_go_backwards(store, monkeypatch):
    mid = str(uuid.uuid4())
    clock = {"t": 1000.0}
    monkeypatch.setattr(pw.time, "time", lambda: clock["t"])
    claim(store, mid, "queued")
    clock["t"] = 900.0  # a skewed clock
    rec = pw.record_wake(store, {"id": mid, "status": "writing"})
    assert rec["state"] == "queued", "writing cannot follow queued"
    assert rec["at"] == 1000.0
    rec = pw.active_hook(store, tool_hook())
    assert rec["state"] == "cancelled" and rec["at"] == 1000.0, "at is max(previous, now)"
    assert pw.record_wake(store, {"id": mid, "status": "uncertain"})["state"] == "cancelled"
    assert store.get("wake_pending") is None, "a decided wake is never re-pended by a late completion"


def test_failed_and_uncertain_keep_pending_until_hook_or_ack(store):
    a = str(uuid.uuid4())
    pw.record_wake(store, {"id": a, "status": "writing"})
    rec = pw.record_wake(store, {"id": a, "status": "failed"})
    assert rec["state"] == "failed"
    assert store.get("wake_pending") == {"id": a, "status": "failed", "at": rec["at"]}, "still pending: no blind retry"
    assert pw.wake_summary(store)["pending"]["status"] == "failed"
    rec = pw.active_hook(store, tool_hook())
    assert rec["state"] == "cancelled" and store.get("wake_pending") is None
    b = str(uuid.uuid4())
    pw.record_wake(store, {"id": b, "status": "writing"})
    rec = pw.record_wake(store, {"id": b, "status": "uncertain"})
    assert rec["state"] == "uncertain" and store.get("wake_pending")["status"] == "uncertain"
    rec = pw.active_hook(store, prompt_hook(notice(b)))
    assert rec["state"] == "activated", "an uncertain RPC that did land can still be proven"
    counts = pw.wake_summary(store)["counts"]
    assert counts["failed"] == 1 and counts["uncertain"] == 1 and counts["cancelled"] == 1 and counts["activated"] == 1
    # caller-side acknowledgement path: deleting the key is enough, the helper does not resurrect it
    c = str(uuid.uuid4())
    claim(store, c, "failed")
    assert pw.wake_summary(store)["pending"]["id"] == c
    with store.db:
        store.db.execute("DELETE FROM meta WHERE key='wake_pending'")
    assert pw.active_hook(store, tool_hook()) is None, "acknowledged: nothing pending"
    assert pw.wake_summary(store)["pending"] is None and pw.wake_summary(store)["last"]["id"] == c


def test_new_wake_replaces_last_record_but_counts_accumulate(store):
    ids = [str(uuid.uuid4()) for _ in range(3)]
    for mid in ids:
        claim(store, mid, "queued")
        pw.active_hook(store, prompt_hook(notice(mid)))
    summary = pw.wake_summary(store)
    assert summary["last"]["id"] == ids[-1]
    assert summary["counts"]["activated"] == 3
    rows = store.db.execute("SELECT key FROM meta").fetchall()
    assert {r[0] for r in rows} == {"last_wake", "wake_counts"}, "exactly two bounded keys, nothing per message"


# --------------------------------------------------------------------------
# input validation, fallback, expiry, summary
# --------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [None, "x", {}, {"id": "nope", "status": "writing"}, {"id": str(uuid.uuid4()), "status": "activated"},
                                 {"id": str(uuid.uuid4()), "status": "queued", "queue_id": 5}])
def test_record_wake_rejects_or_sanitises_bad_markers(store, bad):
    if isinstance(bad, dict) and bad.get("status") == "queued":
        pw.record_wake(store, {"id": bad["id"], "status": "writing"})
        rec = pw.record_wake(store, bad)
        assert rec is not None and rec["queue_id"] is None, "non-string queue id dropped"
    else:
        rec = pw.record_wake(store, bad)
        assert rec is None
        assert store.get("last_wake") is None and store.get("wake_pending") is None


def test_completion_for_never_claimed_id_on_empty_store_is_recorded_once(store):
    # No newer claim exists, so a bare completion is accepted (first record wins),
    # which keeps evidence for a producer whose writing claim was lost.
    mid = str(uuid.uuid4())
    rec = pw.record_wake(store, {"id": mid, "status": "queued", "queue_id": "q"})
    assert rec["id"] == mid and rec["state"] == "queued"


@pytest.mark.parametrize("payload", [None, {}, {"hook_event_name": "Stop"}, {"hook_event_name": "SessionStart", "prompt": "x"}, "str"])
def test_active_hook_ignores_non_active_events(store, payload):
    mid = str(uuid.uuid4())
    claim(store, mid, "queued")
    assert pw.active_hook(store, payload) is None
    assert store.get("wake_pending")["id"] == mid, "untouched"


def test_deleted_pending_key_is_final_no_fallback_to_last_wake(store):
    mid = str(uuid.uuid4())
    claim(store, mid, "queued", "q")
    with store.db:
        store.db.execute("DELETE FROM meta WHERE key='wake_pending'")
    assert pw.active_hook(store, prompt_hook(notice(mid))) is None, "even the exact notice cannot revive an acknowledged wake"
    assert pw.wake_summary(store)["pending"] is None and pw.wake_summary(store)["last"]["state"] == "queued"


def test_active_hook_with_corrupt_pending_uses_last_or_nothing(store):
    store.put("wake_pending", {"id": "garbage"})
    assert pw.active_hook(store, tool_hook()) is None
    store.put("wake_pending", "not-a-dict")
    assert pw.active_hook(store, tool_hook()) is None


def test_late_completion_for_older_wake_never_touches_newer_wake(store):
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    pw.record_wake(store, {"id": a, "status": "writing"})
    pw.record_wake(store, {"id": b, "status": "writing"})  # newer claim
    late = pw.record_wake(store, {"id": a, "status": "queued", "queue_id": "q-a"})
    assert late["id"] == b and late["state"] == "writing", "returns the newer record, unchanged"
    assert store.get("wake_pending")["id"] == b
    assert pw.wake_summary(store)["last"].get("queue_id") is None
    pw.record_wake(store, {"id": b, "status": "queued", "queue_id": "q-b"})
    assert pw.record_wake(store, {"id": a, "status": "failed"})["id"] == b
    assert store.get("wake_pending") == {"id": b, "status": "queued", "at": pw.wake_summary(store)["last"]["at"], "queue_id": "q-b"}
    rec = pw.active_hook(store, prompt_hook(notice(b)))
    assert rec["id"] == b and rec["state"] == "activated"
    assert pw.record_wake(store, {"id": a, "status": "uncertain"})["state"] == "activated"


def test_writing_claim_after_terminal_same_id_does_not_regress(store):
    mid = str(uuid.uuid4())
    claim(store, mid, "queued")
    pw.active_hook(store, tool_hook())
    assert pw.record_wake(store, {"id": mid, "status": "writing"})["state"] == "cancelled"
    assert store.get("wake_pending") is None


def test_notice_text_is_canonical_and_contains_id_once():
    mid = str(uuid.uuid4())
    text = pw.notice_text(mid)
    assert text.startswith(pw.NOTICE_PREFIX)
    assert text.count(mid) == 2 and "peer-chat read --id " + mid in text
    assert "\n" not in text
    assert pw.notice_text(mid) == pw.notice_text(mid)


def test_summary_shape_on_empty_store(store):
    assert pw.wake_summary(store) == {"last": None, "pending": None,
                                      "counts": {"activated": 0, "raced": 0, "cancelled": 0, "failed": 0, "uncertain": 0}}


def test_counts_tolerate_corrupt_meta(store):
    store.put("wake_counts", {"activated": "x", "raced": -3, "bogus": 9, "cancelled": True})
    assert pw.wake_summary(store)["counts"] == {"activated": 0, "raced": 0, "cancelled": 0, "failed": 0, "uncertain": 0}
    mid = str(uuid.uuid4())
    claim(store, mid, "queued")
    pw.active_hook(store, tool_hook())
    assert pw.wake_summary(store)["counts"]["cancelled"] == 1


def test_helper_makes_no_native_or_model_calls():
    src = (ROOT / "peer_wake.py").read_text()
    for forbidden in ("subprocess", "peer_chat", "import os", "socket"):
        assert forbidden not in src, forbidden
    assert "expire" not in src.lower(), "no deadline-based expiry: it would cause blind retries"
    assert set(pw.__all__) >= {"record_wake", "active_hook", "wake_summary", "notice_text"}


def test_same_id_completion_after_explicit_clear_records_cancelled_and_never_repends(store):
    mid = str(uuid.uuid4())
    pw.record_wake(store, {"id": mid, "status": "writing"})
    with store.db:  # ack or owner-change reset cleared the marker while the RPC was in flight
        store.db.execute("DELETE FROM meta WHERE key='wake_pending'")
    rec = pw.record_wake(store, {"id": mid, "status": "queued", "queue_id": "q-late"})
    assert rec["state"] == "cancelled" and rec["queue_id"] == "q-late"
    assert "after pending was cleared" in rec["detail"]
    assert store.get("wake_pending") is None, "never recreated"
    assert pw.wake_summary(store)["pending"] is None
    assert pw.wake_summary(store)["counts"]["cancelled"] == 1
    # the same for a failed/uncertain completion
    other = str(uuid.uuid4())
    pw.record_wake(store, {"id": other, "status": "writing"})
    with store.db:
        store.db.execute("DELETE FROM meta WHERE key='wake_pending'")
    assert pw.record_wake(store, {"id": other, "status": "failed"})["state"] == "cancelled"
    assert store.get("wake_pending") is None


def test_summary_pending_reflects_actual_meta_only(store):
    mid = str(uuid.uuid4())
    claim(store, mid, "queued", "q")
    assert pw.wake_summary(store)["pending"]["id"] == mid
    with store.db:
        store.db.execute("DELETE FROM meta WHERE key='wake_pending'")
    assert pw.wake_summary(store)["pending"] is None
    assert pw.wake_summary(store)["last"]["id"] == mid, "history is kept separately"
