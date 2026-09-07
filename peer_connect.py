"""Discover and connect named existing sessions from either agent."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import peer_platform as pp


def caller_agent():
    """Nearest agent process wins, including nested Codex/Claude terminals.

    Environment variables can be inherited from an outer agent, so they are not
    evidence of which model owns this tool call. The thread id is checked by
    registration once the nearest owner has been identified.
    """
    pid = os.getppid()
    seen = set()
    for _ in range(64):
        if not isinstance(pid, int) or pid <= 1 or pid in seen:
            break
        seen.add(pid)
        name = pp.process_comm(pid)
        if name in ("codex", "claude"):
            return name, pid
        pid = pp.parent_pid(pid)
    return None, None


def all_sessions(state_root):
    from peer_chat import peers
    from peer_registry import sessions
    return list(sessions(state_root)) + [{**r, "kind": "claude", "id": r["session"]} for r in peers()]


def _start(recipient, peer, state_root):
    """Attach one enrollment, preserving other links and existing delivery mode."""
    if pp.process_identity(recipient["owner_pid"]) != recipient["owner_identity"]:
        raise ValueError("Selected Codex process exited; discover sessions again")
    command = [sys.executable, "-m", "peer_chat", "--thread", recipient["thread"], "--state-root", str(state_root),
        "start", "--owner-pid", str(recipient["owner_pid"]), "--owner-identity", recipient["owner_identity"],
        "--delivery", "auto"]
    if peer["kind"] == "codex":
        command += ["--peer-thread", peer["thread"]]
    else:
        command += ["--peer-pid", str(peer["pid"]), "--peer-socket", peer["socket"], "--peer-name", peer["name"] or "Claude"]
    result = subprocess.run(command, env=dict(os.environ, CODEX_HOME=recipient["codex_home"]),
                            capture_output=True, text=True, timeout=20)
    if result.returncode:
        raise ValueError("Connection failed: " + result.stderr.strip()[:500])
    return json.loads(result.stdout)


def _seed_phase(store, phase):
    # A hook may have arrived since discovery. Never overwrite its newer state.
    if phase not in ("idle", "active", "unknown"):
        phase = "unknown"
    with store.db:
        store.db.execute("INSERT OR IGNORE INTO meta VALUES ('phase',?)", (json.dumps(phase),))


def connect(target, state_root, thread=None):
    from peer_chat import Store, peers
    from peer_registry import register, select_session, sessions
    caller_kind, caller_pid = caller_agent()
    claude_pid = caller_pid if caller_kind == "claude" else None
    codex_pid = caller_pid if caller_kind == "codex" else None
    candidates = list(sessions(state_root)) + [{**p, "kind": "claude", "id": p["session"]} for p in peers()]
    peer = select_session(candidates, target)
    if claude_pid:
        caller = next((p for p in candidates if p["kind"] == "claude" and p["pid"] == claude_pid), None)
        if not caller:
            raise ValueError("This Claude session has no live native messaging endpoint")
        if peer["kind"] == "claude":
            if peer["pid"] == claude_pid:
                raise ValueError("Select another session")
            return {"connected": True, "peer": peer["name"], "address": "uds:" + peer["socket"],
                    "transport": "claude-native", "next": "Send using native SendMessage to this address."}
        recipient = peer
        started = _start(recipient, caller, state_root)
        thread = recipient["thread"]
    elif codex_pid and thread:
        home = str(Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")).resolve())
        caller = register({"session_id": thread, "cwd": os.getcwd()}, home, state_root, owner_pid=codex_pid)
        if not caller:
            raise ValueError("Could not identify this Codex session")
        if peer["kind"] == "codex" and peer["thread"] == thread:
            raise ValueError("Select another session")
        recipient = caller
        started = _start(caller, peer, state_root)
        if peer["kind"] == "codex":
            try:
                _start(peer, caller, state_root)
            except ValueError as exc:
                raise ValueError("Reciprocal connection incomplete; the first enrollment is retained for retry. " + str(exc)) from exc
            other_store = Store(Path(state_root) / peer["thread"] / "inbox.sqlite")
            try:
                _seed_phase(other_store, peer.get("phase", "unknown"))
            finally:
                other_store.close()
    else:
        raise ValueError("Run connect from the intended Codex or Claude session")
    store = Store(Path(state_root) / thread / "inbox.sqlite")
    try:
        _seed_phase(store, recipient.get("phase", "unknown") if claude_pid else "active")
        if not claude_pid:
            # Only the initiating session changes its default destination. Replies
            # always choose the sender of --reply-to, even with multiple links.
            with store.db:
                store.db.execute("BEGIN IMMEDIATE")
                config = store.get("config")
                config["default_peer"] = started["peer_key"]
                store.db.execute("UPDATE meta SET value=? WHERE key='config'", (json.dumps(config),))
        from peer_budget import delivery_state, lifecycle_observed
        address = "uds:" + store.get("config")["socket"]
        phase = store.get("phase", "unknown")
        recipient_phase = phase
        recipient_delivery = delivery_state(store)
        recipient_hooks = lifecycle_observed(store)
        if not claude_pid and peer["kind"] == "codex":
            destination = Store(Path(state_root) / peer["thread"] / "inbox.sqlite")
            try:
                recipient_phase = destination.get("phase", "unknown")
                recipient_delivery = delivery_state(destination)
                recipient_hooks = lifecycle_observed(destination)
            finally:
                destination.close()
        next_step = "Send using native SendMessage to this address." if claude_pid else "Send with peer-chat send --to SESSION_NAME --file PATH; use --reply-to MESSAGE_ID for replies."
        if not recipient_hooks:
            next_step += " Messages are retained in the inbox until Codex runs its hooks; connecting does not wake an untouched tab."
        from peer_observe import snapshot
        destination_thread = peer['thread'] if peer['kind'] == 'codex' else thread
        destination_status = snapshot(state_root, destination_thread)
        return {"connected": True, "destination": destination_status, "thread": thread, "peer": peer["name"], "address": address,
            "delivery": store.get("delivery"), "phase": phase, "delivery_state": delivery_state(store),
            "recipient_phase": recipient_phase, "recipient_delivery_state": recipient_delivery,
            "model_delivery": "lifecycle_observed" if recipient_hooks else "awaiting_lifecycle_hook", "next": next_step}
    finally:
        store.close()
