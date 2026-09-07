"""Connection must not confuse discovery with model lifecycle or lose a racing hook."""
import json
import subprocess
import uuid

import pytest

import peer_chat
import peer_connect
import peer_registry


@pytest.mark.parametrize("existing_phase", [None, "active", "idle"])
@pytest.mark.parametrize("hook_seen", [False, True])
def test_discovered_empty_session_connects_without_prompt_or_guessed_idle(tmp_path, monkeypatch, existing_phase, hook_seen):
    thread = str(uuid.uuid4())
    state = tmp_path / "state"
    state.joinpath(thread).mkdir(parents=True)
    row = {"id": thread, "thread": thread, "kind": "codex", "name": "empty-tab",
           "owner_pid": 123, "owner_identity": "stable-owner", "codex_home": str(tmp_path / "home"),
           "source": "writer-lock", "phase": "unknown", "initialization_required": True}
    monkeypatch.setattr(peer_registry, "sessions", lambda root: [row])
    monkeypatch.setattr(peer_connect, "caller_agent", lambda: ("claude", 456))
    monkeypatch.setattr(peer_connect.pp, "process_identity", lambda pid: "stable-owner")
    monkeypatch.setattr(peer_chat, "peers", lambda: [{"pid": 456, "name": "reviewer", "socket": "/peer.sock", "session": "11111111-1111-4111-8111-111111111111"}])
    calls = []

    def start(command, **kwargs):
        calls.append(command)
        assert command[command.index("start") + 1] == "--owner-pid"
        store = peer_chat.Store(state / thread / "inbox.sqlite")
        store.put("config", {"socket": "/bridge.sock"})
        store.put("delivery", "auto")
        store.put("remaining", 12)
        if hook_seen:
            store.put("hook_seen", {"at": 123.0, "event": "PostToolUse", "turn": "turn-1", "routing": "matched"})
        if existing_phase is not None:
            # A lifecycle hook can run between discovery and startup completion.
            store.put("phase", existing_phase)
        store.close()
        return subprocess.CompletedProcess(command, 0, json.dumps({"status": "running"}), "")

    monkeypatch.setattr(peer_connect.subprocess, "run", start)
    result = peer_connect.connect("empty-tab", state)
    assert result["connected"] is True and result["address"] == "uds:/bridge.sock"
    assert result["phase"] == (existing_phase or "unknown")
    assert len(calls) == 1  # Only transport startup; no prompt or queue call.
    store = peer_chat.Store(state / thread / "inbox.sqlite")
    assert store.get("phase") == (existing_phase or "unknown")
    assert store.get("wake_pending") is None
    store.close()
    if not hook_seen:
        assert result["model_delivery"] == "awaiting_lifecycle_hook"
        assert "does not wake an untouched tab" in result["next"]
    else:
        assert result["model_delivery"] == "lifecycle_observed"


def test_selected_owner_exiting_before_connect_is_not_replaced(tmp_path, monkeypatch):
    thread = str(uuid.uuid4())
    monkeypatch.setattr(peer_registry, "sessions", lambda root: [{
        "id": thread, "thread": thread, "kind": "codex", "name": "empty-tab",
        "owner_pid": 123, "owner_identity": "old-owner", "codex_home": str(tmp_path)}])
    monkeypatch.setattr(peer_connect, "caller_agent", lambda: ("claude", 456))
    monkeypatch.setattr(peer_connect.pp, "process_identity", lambda pid: "replacement-owner")
    monkeypatch.setattr(peer_chat, "peers", lambda: [{"pid": 456, "name": "reviewer", "socket": "/peer.sock", "session": "11111111-1111-4111-8111-111111111111"}])
    monkeypatch.setattr(peer_connect.subprocess, "run", lambda *a, **k: pytest.fail("must not start transport"))
    with pytest.raises(ValueError, match="exited"):
        peer_connect.connect("empty-tab", tmp_path)


@pytest.mark.parametrize("near,far", [("codex", "claude"), ("claude", "codex")])
def test_nearest_agent_wins_over_outer_agent_and_inherited_environment(monkeypatch, near, far):
    monkeypatch.setenv("CODEX_THREAD_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setattr(peer_connect.os, "getppid", lambda: 30)
    monkeypatch.setattr(peer_connect.pp, "parent_pid", lambda pid: {30: 20, 20: 10, 10: 1}.get(pid))
    monkeypatch.setattr(peer_connect.pp, "process_comm", lambda pid: {30: "bash", 20: near, 10: far}.get(pid))
    assert peer_connect.caller_agent() == (near, 20)
