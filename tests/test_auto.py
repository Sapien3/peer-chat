import json
from pathlib import Path
import subprocess
import uuid

import pytest

import peer_chat
import peer_hooks
import peer_setup


@pytest.fixture
def auto_store(tmp_path):
    store = peer_chat.Store(tmp_path / "inbox.sqlite")
    config = {"thread": str(uuid.uuid4()), "codex_bin": "synthetic-codex",
              "peer": {"identity": "peer", "name": "Reviewer", "pid": 1, "socket": "/synthetic.sock"}}
    store.put("config", config)
    store.put("delivery", "auto")
    store.put("remaining", 5)
    mid = str(uuid.uuid4())
    store.accept("peer", {"id": mid, "kind": "message", "body": "PRIVATE BODY NEVER IN QUEUE",
                          "sender_mode": "prompting", "hops": []})
    yield store, config, mid
    store.close()


def test_idle_wake_queues_only_one_notice_and_preserves_body_for_hook(auto_store, monkeypatch):
    store, config, mid = auto_store
    store.put("phase", "idle")
    calls = []
    monkeypatch.setattr(peer_chat.subprocess, "run", lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args, 0, f"Queued message {uuid.uuid4()}", ""))
    peer_chat.dispatch(store, config)
    peer_chat.dispatch(store, config)
    assert len(calls) == 1 and mid in calls[0][-1]
    assert "PRIVATE BODY" not in calls[0][-1]
    assert store.pending()[0]["id"] == mid
    assert store.get("remaining") == 5
    assert store.get("wake_pending")["status"] == "queued"


@pytest.mark.parametrize("phase,budget", [("active", 5), ("unknown", 5), ("idle", 0)])
def test_no_wake_during_work_unknown_phase_or_exhaustion(auto_store, monkeypatch, phase, budget):
    store, config, _ = auto_store
    store.put("phase", phase)
    store.put("remaining", budget)
    monkeypatch.setattr(peer_chat.subprocess, "run", lambda *a, **k: pytest.fail("unexpected queue"))
    peer_chat.dispatch(store, config)
    assert store.pending()


def test_prompt_racing_wake_keeps_inbox_deliverable_and_does_not_resurrect_marker(auto_store, monkeypatch):
    store, config, _ = auto_store
    store.put("phase", "idle")
    def run(args, **kwargs):
        store.put("phase", "active")
        store.put("wake_pending", None)
        return subprocess.CompletedProcess(args, 0, "Queued message " + str(uuid.uuid4()), "")
    monkeypatch.setattr(peer_chat.subprocess, "run", run)
    peer_chat.dispatch(store, config)
    assert store.get("wake_pending") is None
    assert store.pending()


def test_failed_wake_does_not_spin_or_lose_message(auto_store, monkeypatch):
    store, config, _ = auto_store
    store.put("phase", "idle")
    calls = []
    monkeypatch.setattr(peer_chat.subprocess, "run", lambda a, **k: calls.append(a) or subprocess.CompletedProcess(a, 1, "", ""))
    peer_chat.dispatch(store, config)
    peer_chat.dispatch(store, config)
    assert len(calls) == 1 and store.pending()
    assert store.get("wake_pending")["status"] == "failed"


def test_manual_ack_clears_wake_but_does_not_replenish_wake_budget(auto_store, monkeypatch):
    store, config, _ = auto_store
    store.put("phase", "idle")
    monkeypatch.setattr(peer_chat.subprocess, "run", lambda a, **k: subprocess.CompletedProcess(a, 0, "", ""))
    peer_chat.dispatch(store, config)
    store.read(True)
    assert store.get("wake_pending") is None
    assert store.get("wake_remaining") == 4


def hook_rows(home, command):
    return [{"key": str(home / "hooks.json") + ":" + event, "eventName": event,
             "handlerType": "command", "command": command, "async": False,
             "matcher": None, "timeoutSec": 3, "statusMessage": peer_setup.MARKER,
             "sourcePath": str(home / "hooks.json"), "isManaged": False, "enabled": True,
             "currentHash": "sha256:" + str(i) * 64, "trustStatus": "untrusted"}
            for i, event in enumerate(("sessionStart", "postToolUse", "userPromptSubmit", "stop"))]


def test_consent_pins_exact_definitions_and_preserves_unrelated_hooks(tmp_path):
    command = "/installed/peer-chat-hook"
    class Client:
        def __init__(self):
            self.rows = hook_rows(tmp_path, command)
            self.calls = []
        def hooks(self):
            return self.rows + [{"statusMessage": "unrelated"}]
        def call(self, method, params):
            self.calls.append((method, params))
            if method == "config/read":
                return {"layers": []}
            for row in self.rows:
                row["trustStatus"] = "trusted"
            return {}
    client = Client()
    digest = peer_setup.review_digest(peer_setup.review_hooks(client, tmp_path, command))
    with pytest.raises(ValueError, match="changed"):
        peer_setup.enable_reviewed(client, tmp_path, command, "unapproved")
    assert not client.calls
    peer_setup.enable_reviewed(client, tmp_path, command, digest)
    writes = [p for m, p in client.calls if m == "config/batchWrite"]
    assert len(writes) == 1 and len(writes[0]["edits"]) == 4
    assert all(e["keyPath"].endswith('.trusted_hash') for e in writes[0]["edits"])
    assert not any("sandbox" in e["keyPath"] for e in writes[0]["edits"])
    client.rows[0]["command"] = "/unreviewed/other-hook"
    with pytest.raises(ValueError, match="differs"):
        peer_setup.enable_reviewed(client, tmp_path, command, digest)


@pytest.mark.parametrize('with_next', [False, True])
def test_idle_hop_guard_marks_held_and_does_not_block_later_work(auto_store, monkeypatch, with_next):
    store, config, mid = auto_store
    store.put('phase', 'idle')
    with store.db:
        store.db.execute('UPDATE messages SET hops=? WHERE id=?', (json.dumps(['a' * 24] * 8), mid))
    next_id = str(uuid.uuid4())
    if with_next:
        store.accept('peer', {'id': next_id, 'kind': 'message', 'body': 'Other authorized work', 'hops': []})
    calls = []
    monkeypatch.setattr(peer_chat.subprocess, 'run', lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args, 0, '', ''))
    peer_chat.dispatch(store, config)
    peer_chat.dispatch(store, config)
    row = store.db.execute('SELECT status,detail FROM messages WHERE id=?', (mid,)).fetchone()
    assert tuple(row) == ('held', 'Hop limit reached')
    assert store.get('remaining') == 5
    assert store.get('wake_remaining', 5) == (4 if with_next else 5)
    assert len(calls) == int(with_next)
    if with_next:
        assert next_id in calls[0][-1] and mid not in calls[0][-1]
