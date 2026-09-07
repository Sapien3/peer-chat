"""Peer enrollment table and endpoint resolution (pure; no sockets opened,
no registry or backend mutations, no model launches).

``config["peers"]`` is a mapping keyed by *identity*, never by display name:

- Claude peers: key == the Claude process identity string, exactly the value
  legacy ``config["peer"]["identity"]`` used as the ``messages.peer`` key, so
  existing inboxes need no migration.
- Codex peers: key == ``"codex:" + sha256(canonical codex_home)[:16] + ":" + thread``.

If ``config["peers"]`` exists it is the authority; a conflicting legacy
``config["peer"]`` is ignored. A legacy-only config is normalised on read.
``enroll`` keeps ``config["peer"]`` as a mirror of the first Claude peer so
already-running old daemons keep working until the core stops using it.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple

import peer_platform as pp

DB_TIMEOUT = 0.2
KINDS = ("claude", "codex")


def valid_uuid(value) -> bool:
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value.lower() and value == value.lower()
    except ValueError:
        return False


def _absolute_root(value) -> Optional[str]:
    """Non-empty absolute path, resolved; None otherwise (never Path("") -> cwd)."""
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        return None
    return str(path.resolve())


# --------------------------------------------------------------------------
# keys and records
# --------------------------------------------------------------------------


def canonical_home(codex_home) -> str:
    return str(Path(codex_home).expanduser().resolve())


def codex_key(codex_home, thread: str) -> str:
    if not valid_uuid(thread):
        raise ValueError("Codex thread must be a canonical lowercase UUID")
    digest = hashlib.sha256(canonical_home(codex_home).encode()).hexdigest()[:16]
    return f"codex:{digest}:{thread}"


def describe_peer(record) -> str:
    if not isinstance(record, dict):
        return "?"
    if record.get("kind") == "codex":
        return f"codex {str(record.get('thread', ''))[:8]} {record.get('name', '')} home={record.get('codex_home', '-')}"
    return f"claude pid={record.get('pid', '?')} {record.get('name', '')} {record.get('socket', '-')}"


def claude_record(native_peer, *, enrolled_by: str = "connect") -> dict:
    """Pin a live Claude session (row from peer_chat.peers()) as a peer record."""
    if not isinstance(native_peer, dict):
        raise ValueError("Claude peer must be a discovery row")
    pid = native_peer.get("pid")
    socket_path = native_peer.get("socket")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError("Claude peer row has no valid pid")
    if not isinstance(socket_path, str) or not socket_path:
        raise ValueError("Claude peer row has no socket path")
    identity = native_peer.get("identity") if isinstance(native_peer.get("identity"), str) else pp.process_identity(pid)
    if identity is None or pp.process_identity(pid) != identity:
        raise ValueError("Claude peer process is not live")
    record = {"kind": "claude", "key": identity, "name": str(native_peer.get("name") or "Claude"),
              "pid": pid, "identity": identity, "socket": str(Path(socket_path).absolute()),
              "enrolled_at": time.time(), "enrolled_by": enrolled_by}
    if isinstance(native_peer.get("session"), str):
        record["session"] = native_peer["session"]
    return record


def codex_record(registry_row, state_root, *, name: Optional[str] = None, enrolled_by: str = "connect") -> dict:
    """Canonical record for a Codex thread from a peer_registry.sessions() row."""
    if not isinstance(registry_row, dict) or registry_row.get("kind") != "codex":
        raise ValueError("Codex peer must be a registry row of kind codex")
    thread = registry_row.get("thread") or registry_row.get("id")
    home = registry_row.get("codex_home")
    if not valid_uuid(thread):
        raise ValueError("Codex registry row thread is not a canonical UUID")
    if not isinstance(home, str) or not home.strip() or not Path(home).expanduser().is_absolute():
        raise ValueError("Codex registry row codex_home must be an absolute path")
    root = _absolute_root(str(state_root) if state_root is not None else None)
    if root is None:
        raise ValueError("state_root must be a non-empty absolute path")
    home = canonical_home(home)
    record = {"kind": "codex", "key": codex_key(home, thread), "name": str(name or registry_row.get("name") or thread[:8]),
              "thread": thread, "codex_home": home, "state_root": root,
              "enrolled_at": time.time(), "enrolled_by": enrolled_by}
    if isinstance(registry_row.get("owner_identity"), str):
        record["owner_identity"] = registry_row["owner_identity"]
    return record


def _normalise(key, record) -> Optional[dict]:
    """Canonical record or None. Kind allowlist: "codex", "claude", or absent
    (legacy claude-shaped entries); any other kind is rejected."""
    if not isinstance(record, dict):
        return None
    kind = record.get("kind")
    if kind is not None and kind not in KINDS:
        return None
    out = dict(record)
    if kind == "codex":
        home = out.get("codex_home")
        if (not valid_uuid(out.get("thread")) or not isinstance(home, str) or not home.strip()
                or not Path(home).expanduser().is_absolute()):
            return None
        root = _absolute_root(out.get("state_root"))
        if root is None:
            return None
        out["codex_home"] = canonical_home(home)
        out["state_root"] = root
        out["key"] = codex_key(out["codex_home"], out["thread"])
    else:
        if (not isinstance(out.get("identity"), str) or not out["identity"]
                or not isinstance(out.get("pid"), int) or isinstance(out.get("pid"), bool) or out["pid"] <= 0
                or not isinstance(out.get("socket"), str) or not out["socket"]):
            return None
        out["kind"] = "claude"
        out["key"] = out["identity"]
    out.setdefault("name", out["key"][:8])
    return out


def all_peers(config) -> dict:
    """Canonical key->record map. ``peers`` is the authority; legacy ``peer`` only when no map exists."""
    if not isinstance(config, dict):
        return {}
    peers = config.get("peers")
    if isinstance(peers, dict):
        out = {}
        for key, record in peers.items():
            normalised = _normalise(key, record)
            if normalised is not None:
                out[normalised["key"]] = normalised
        return out
    legacy = config.get("peer")
    if isinstance(legacy, dict):
        normalised = _normalise(None, {"kind": "claude", "pid": legacy.get("pid"), "identity": legacy.get("identity"),
                                       "socket": legacy.get("socket"), "name": legacy.get("name") or "Claude",
                                       "enrolled_by": "legacy"})
        if normalised is not None:
            return {normalised["key"]: normalised}
    return {}


def _legacy_mirror(record) -> dict:
    return {"pid": record["pid"], "identity": record["identity"], "socket": record["socket"], "name": record.get("name", "Claude")}


def enroll(store, record) -> dict:
    """Atomically merge one record into config.peers; returns the new config.

    Preserves every other peer, keeps config.peer mirrored to the first Claude
    peer for old daemons, and touches no other meta key.
    """
    normalised = _normalise(None, record)
    if normalised is None:
        raise ValueError("Peer record is incomplete")
    with store.db:
        store.db.execute("BEGIN IMMEDIATE")
        row = store.db.execute("SELECT value FROM meta WHERE key='config'").fetchone()
        config = json.loads(row[0]) if row and row[0] else {}
        if not isinstance(config, dict):
            config = {}
        peers = all_peers(config)
        peers[normalised["key"]] = normalised
        config["peers"] = peers
        first_claude = next((p for p in peers.values() if p["kind"] == "claude"), None)
        if first_claude is not None:
            config["peer"] = _legacy_mirror(first_claude)
        store.db.execute("INSERT OR REPLACE INTO meta VALUES ('config', ?)", (json.dumps(config),))
    return config


# --------------------------------------------------------------------------
# resolution
# --------------------------------------------------------------------------


def _check_socket(path, what: str) -> str:
    try:
        info = Path(path).lstat()
    except OSError:
        raise ValueError(f"{what}: socket path does not exist")
    if not stat.S_ISSOCK(info.st_mode):
        raise ValueError(f"{what}: path is not a socket")
    if info.st_uid != os.getuid():
        raise ValueError(f"{what}: socket belongs to another user")
    return str(path)


def _read_counterpart(db_path: Path) -> Tuple[dict, dict]:
    try:
        info = db_path.stat()
    except OSError:
        raise ValueError("codex peer: counterpart bridge state not found")
    if info.st_uid != os.getuid():
        raise ValueError("codex peer: counterpart bridge state belongs to another user")
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=DB_TIMEOUT)
    except sqlite3.Error:
        raise ValueError("codex peer: counterpart bridge state unreadable")
    try:
        meta = {}
        for key in ("config", "runtime"):
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            meta[key] = json.loads(row[0]) if row and row[0] else None
    except (sqlite3.Error, ValueError):
        raise ValueError("codex peer: counterpart bridge metadata corrupt or busy")
    finally:
        conn.close()
    config, runtime = meta.get("config"), meta.get("runtime")
    if not isinstance(config, dict):
        raise ValueError("codex peer: counterpart has no bridge configuration")
    if not isinstance(runtime, dict):
        raise ValueError("codex peer: counterpart bridge is not running")
    return config, runtime


def _owner_verified(record: dict, owner_identity: str, state_root: Path) -> bool:
    """Is owner_identity the live, verified owner of this thread+home? Cheap: one file each."""
    thread, home = record["thread"], record["codex_home"]
    reg_path = state_root / "registry" / f"{thread}.json"
    if reg_path.is_file() and not reg_path.is_symlink():
        try:
            data = json.loads(reg_path.read_text())
        except (OSError, ValueError):
            data = None
        if (isinstance(data, dict) and data.get("thread") == thread
                and canonical_home(data.get("codex_home") or "") == home
                and data.get("owner_identity") == owner_identity
                and isinstance(data.get("owner_pid"), int)
                and pp.process_identity(data["owner_pid"]) == owner_identity):
            return True
    lock = Path(home) / "thread-writer-locks" / f"{thread}.lock"
    owner = pp.exclusive_lock_owner(lock, comm="codex")
    return bool(owner and owner["identity"] == owner_identity)


def resolve_peer(record, *, state_root=None) -> dict:
    """Live endpoint {pid, identity, socket} for a record, or ValueError with a bounded reason."""
    if isinstance(record, dict) and record.get("kind") == "codex":
        if _absolute_root(str(state_root) if state_root is not None else record.get("state_root")) is None:
            raise ValueError("codex peer: state_root must be a non-empty absolute path")
    normalised = _normalise(None, record)
    if normalised is None:
        raise ValueError("peer record is incomplete")
    if normalised["kind"] == "claude":
        if pp.process_identity(normalised["pid"]) != normalised["identity"]:
            raise ValueError("claude peer: process is not live or was replaced")
        return {"pid": normalised["pid"], "identity": normalised["identity"],
                "socket": _check_socket(normalised["socket"], "claude peer")}
    root_str = _absolute_root(str(state_root) if state_root is not None else normalised.get("state_root"))
    if root_str is None:
        raise ValueError("codex peer: state_root must be a non-empty absolute path")
    root = Path(root_str)
    config, runtime = _read_counterpart(root / normalised["thread"] / "inbox.sqlite")
    if config.get("thread") != normalised["thread"]:
        raise ValueError("codex peer: counterpart bridge serves a different thread")
    if canonical_home(config.get("codex_home") or "") != normalised["codex_home"]:
        raise ValueError("codex peer: counterpart bridge uses a different Codex home")
    pid, identity = runtime.get("pid"), runtime.get("identity")
    if not isinstance(pid, int) or isinstance(pid, bool) or not isinstance(identity, str) or pp.process_identity(pid) != identity:
        raise ValueError("codex peer: counterpart listener is not live")
    if pp.process_uid(pid) not in (None, os.getuid()):
        raise ValueError("codex peer: counterpart listener runs as another user")
    owner_pid = config.get("owner_pid")
    if not isinstance(owner_pid, int) or isinstance(owner_pid, bool) or pp.process_identity(owner_pid) != config.get("owner_identity"):
        raise ValueError("codex peer: counterpart owner process is not live or was replaced")
    socket_path = config.get("socket")
    if not isinstance(socket_path, str) or not socket_path or runtime.get("socket") not in (None, socket_path):
        raise ValueError("codex peer: counterpart socket path is inconsistent")
    owner_identity = config.get("owner_identity")
    if not isinstance(owner_identity, str) or not _owner_verified(normalised, owner_identity, root):
        raise ValueError("codex peer: counterpart owner is not the live verified owner of that thread")
    return {"pid": pid, "identity": identity, "socket": _check_socket(socket_path, "codex peer")}


def expected_endpoints(config, *, state_root=None) -> Tuple[dict, list]:
    """{pid: record} for every resolvable peer plus [(key, reason)] for the rest."""
    endpoints, failures = {}, []
    for key, record in all_peers(config).items():
        try:
            endpoint = resolve_peer(record, state_root=state_root)
        except ValueError as exc:
            failures.append((key, str(exc)))
            continue
        if endpoint["pid"] in endpoints:
            raise ValueError(f"two enrolled peers resolve to the same process pid {endpoint['pid']}: "
                             f"{endpoints[endpoint['pid']]['key']} and {key}")
        endpoints[endpoint["pid"]] = dict(record, endpoint=endpoint)
    return endpoints, failures


# --------------------------------------------------------------------------
# selection
# --------------------------------------------------------------------------


def select_peer(config, selector) -> dict:
    """Exactly one record by key, name, case-insensitive name, or key/thread prefix (>= 8)."""
    peers = all_peers(config)
    if not isinstance(selector, str) or not selector.strip():
        raise ValueError("A peer selector is required; candidates: " + ("; ".join(describe_peer(r) for r in peers.values()) or "none"))
    selector = selector.strip()
    strategies = [
        lambda r: r["key"] == selector,
        lambda r: r.get("name") == selector,
        lambda r: isinstance(r.get("name"), str) and r["name"].lower() == selector.lower(),
        lambda r: len(selector) >= 8 and (r["key"].startswith(selector) or str(r.get("thread", "")).startswith(selector)),
    ]
    for strategy in strategies:
        matches = [r for r in peers.values() if strategy(r)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(f"Selector {selector!r} is ambiguous; candidates: " + "; ".join(describe_peer(r) for r in matches))
    raise ValueError(f"No enrolled peer matches {selector!r}; candidates: " + ("; ".join(describe_peer(r) for r in peers.values()) or "none"))


__all__ = ["all_peers", "enroll", "codex_record", "claude_record", "resolve_peer", "expected_endpoints",
           "select_peer", "describe_peer", "codex_key", "canonical_home", "valid_uuid", "KINDS"]
