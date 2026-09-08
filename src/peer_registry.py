"""Presence registry for Codex sessions that can be invited into peer-chat.

A registration says "this Codex thread is alive, here is how to identify its
owning process". It grants nothing: permission metadata is recorded only when
Codex's own state database proves it, and is otherwise ``"unknown"``.

Files: ``<state_root>/registry/<thread-uuid>.json`` (dir 0700, file 0600,
atomic replace). ``sessions()`` is read-only; ``prune()`` is the only deleter.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

import peer_platform as pp

VERSION = 1
REGISTRATION_EVENTS = ("SessionStart", "UserPromptSubmit", "PostToolUse", "Stop")
IDLE_EVENTS = ("SessionStart", "Stop")
ACTIVE_EVENTS = ("UserPromptSubmit", "PostToolUse")
PHASES = ("idle", "active", "unknown")
SUBAGENT_MARKERS = ("agent_id", "agent_type")
NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
MAX_NAME = 64
DB_TIMEOUT = 0.2


class SessionList(list):
    """A list of live session rows; ``skipped`` counts unreadable registrations."""

    skipped: int = 0


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def valid_id(value) -> bool:
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value.lower()
    except ValueError:
        return False


def registry_dir(state_root) -> Path:
    return Path(state_root) / "registry"


def _private_dir(path: Path) -> Path:
    if not path.parent.exists():
        _private_dir(path.parent)
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("Registry directory has unexpected type or owner")
    path.chmod(0o700)
    return path


def _atomic_write_json(path: Path, data: dict) -> None:
    if path.is_symlink():
        raise ValueError("Refusing to write through a symlink")
    fd, tmp = tempfile.mkstemp(prefix=".reg-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(data, stream, ensure_ascii=False, sort_keys=True)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)


def sanitise_name(value, fallback: str) -> str:
    text = NAME_RE.sub("-", str(value or "")).strip("-._")
    return (text or fallback)[:MAX_NAME]


def default_name(cwd, thread: str) -> str:
    base = Path(cwd).name if cwd else ""
    return sanitise_name(f"{base}-{thread[:8]}" if base else thread[:8], thread[:8])


def _thread_metadata(codex_home, thread: str) -> dict:
    """Read-only look at state_5.sqlite; every failure degrades to unknown."""
    meta = {"cwd": None, "model": None, "rollout_path": None, "permission_class": "unknown", "is_child": False, "found": False}
    db_path = Path(codex_home) / "state_5.sqlite"
    if not db_path.is_file():
        return meta
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=DB_TIMEOUT)
    except sqlite3.Error:
        return meta
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(threads)")}
        wanted = [c for c in ("cwd", "model", "rollout_path", "sandbox_policy", "approval_mode", "archived") if c in cols]
        if "sandbox_policy" not in wanted or "approval_mode" not in wanted:
            return meta
        row = conn.execute(f"SELECT {', '.join(wanted)} FROM threads WHERE id=?", (thread,)).fetchone()
        if row:
            meta["found"] = True
            values = dict(zip(wanted, row))
            meta["cwd"] = values.get("cwd") or None
            meta["model"] = values.get("model") or None
            meta["rollout_path"] = values.get("rollout_path") or None
            if not values.get("archived"):
                meta["permission_class"] = _classify(values.get("sandbox_policy"), values.get("approval_mode"))
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='thread_spawn_edges'").fetchone():
            if conn.execute("SELECT 1 FROM thread_spawn_edges WHERE child_thread_id=? LIMIT 1", (thread,)).fetchone():
                meta["is_child"] = True
    except sqlite3.Error:
        return meta
    finally:
        conn.close()
    return meta


def _classify(sandbox, approval) -> str:
    """Mirror peer_chat.permission_class without raising; never guesses bypass."""
    try:
        kind = json.loads(sandbox)["type"]
    except (TypeError, ValueError, KeyError):
        return "unknown"
    if kind in ("disabled", "danger-full-access") and approval == "never":
        return "bypass"
    if kind in ("disabled", "danger-full-access", "read-only", "workspace-write") and approval in (
        "on-request", "on-failure", "untrusted"
    ):
        return "prompting"
    return "unknown"


# --------------------------------------------------------------------------
# register
# --------------------------------------------------------------------------


def register(payload, codex_home, state_root, *, owner_pid: Optional[int] = None, name: Optional[str] = None,
             phase: Optional[str] = None) -> Optional[dict]:
    """Record a live main-thread Codex session. Returns the record or None; never raises for bad input.

    phase: for explicit registrations only; hook events set it themselves
    (SessionStart/Stop => idle, UserPromptSubmit/PostToolUse => active).
    """
    if not isinstance(payload, dict):
        return None
    thread = payload.get("session_id")
    if not valid_id(thread):
        return None
    if any(payload.get(key) for key in SUBAGENT_MARKERS) or payload.get("agent_transcript_path") is not None:
        return None
    event = payload.get("hook_event_name")
    if event is not None and event not in REGISTRATION_EVENTS:
        return None
    env_thread = os.environ.get("CODEX_THREAD_ID")
    if env_thread and env_thread != thread:
        return None  # a child hook running under the parent thread id, or the wrong session
    if event in IDLE_EVENTS:
        phase = "idle"
    elif event in ACTIVE_EVENTS:
        phase = "active"
    elif phase not in PHASES:
        phase = "unknown"
    pid = owner_pid if owner_pid is not None else pp.find_owner_pid(os.getppid())
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return None
    identity = pp.process_identity(pid)
    if identity is None:
        return None
    meta = _thread_metadata(codex_home, thread)
    if meta["is_child"]:
        return None
    transcript = payload.get("transcript_path")
    if isinstance(transcript, str) and transcript and meta["rollout_path"]:
        if Path(transcript).resolve() != Path(meta["rollout_path"]).resolve():
            return None  # hook transcript belongs to another thread than the one it names
    cwd = payload.get("cwd") if isinstance(payload.get("cwd"), str) and payload.get("cwd") else meta["cwd"]
    permission_mode = payload.get("permission_mode")
    model = payload.get("model") if isinstance(payload.get("model"), str) and payload.get("model") else meta["model"]

    reg_dir = _private_dir(registry_dir(state_root))
    path = reg_dir / f"{thread}.json"
    now = time.time()
    registered_at = now
    previous_name = None
    if path.is_file() and not path.is_symlink():
        try:
            previous = json.loads(path.read_text())
            if isinstance(previous, dict) and isinstance(previous.get("registered_at"), (int, float)):
                registered_at = previous["registered_at"]
            # The name is a label that follows the thread UUID across resume and
            # new owner pids (Claude connects by name). It carries no authority:
            # owner/peer identity checks live in the bridge, not here.
            if (isinstance(previous, dict) and previous.get("thread") == thread
                    and previous.get("codex_home") == str(Path(codex_home))
                    and isinstance(previous.get("name"), str) and previous["name"]):
                previous_name = previous["name"]
        except (OSError, ValueError):
            pass
    exe = pp.process_exe(pid)
    record = {
        "version": VERSION,
        "kind": "codex",
        "thread": thread,
        "name": sanitise_name(name, default_name(cwd, thread)) if name else (previous_name or default_name(cwd, thread)),
        "cwd": cwd,
        "owner_pid": pid,
        "owner_identity": identity,
        "exe": str(exe) if exe else None,
        "codex_home": str(Path(codex_home)),
        "transcript_path": transcript if isinstance(transcript, str) and transcript else None,
        "permission_mode": permission_mode if isinstance(permission_mode, str) and permission_mode else None,
        "permission_class": meta["permission_class"],
        "model": model,
        "phase": phase,
        "source": f"hook:{event}" if event else "explicit",
        "platform": pp.supported_platform()["os"],
        "registered_at": registered_at,
        "updated_at": now,
    }
    _atomic_write_json(path, record)
    return record


# --------------------------------------------------------------------------
# sessions / prune
# --------------------------------------------------------------------------


def _read_record(path: Path) -> Optional[dict]:
    if path.is_symlink() or not path.is_file():
        return None
    info = path.stat()
    if info.st_uid != os.getuid():
        return None
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("version") != VERSION or data.get("kind") != "codex":
        return None
    if not valid_id(data.get("thread")) or path.stem != data["thread"]:
        return None
    if not isinstance(data.get("owner_pid"), int) or not isinstance(data.get("owner_identity"), str):
        return None
    return data


def _alive(record: dict) -> bool:
    return pp.process_identity(record["owner_pid"]) == record["owner_identity"]


def _bridge_info(state_root, thread: str) -> Optional[dict]:
    """Peek at an existing bridge inbox for this thread; read-only, tolerant."""
    db_path = Path(state_root) / thread / "inbox.sqlite"
    if not db_path.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=DB_TIMEOUT)
    except sqlite3.Error:
        return None
    try:
        meta = {}
        for key in ("config", "runtime", "delivery"):
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            meta[key] = json.loads(row[0]) if row and row[0] else None
    except (sqlite3.Error, ValueError):
        return None
    finally:
        conn.close()
    config = meta.get("config")
    if not isinstance(config, dict):
        return None
    runtime = meta.get("runtime")
    runtime_alive = bool(isinstance(runtime, dict) and pp.process_identity(runtime.get("pid")) == runtime.get("identity"))
    peer = config.get("peer") if isinstance(config.get("peer"), dict) else {}
    from peer_peers import all_peers
    return {
        "peers": [{"key": key, "kind": record["kind"], "name": record["name"]}
                  for key, record in all_peers(config).items()],
        "socket": config.get("socket"),
        "peer_pid": peer.get("pid"),
        "peer_name": peer.get("name"),
        "delivery": meta.get("delivery"),
        "runtime_alive": runtime_alive,
        "owner_pid": config.get("owner_pid"),
        "owner_identity": config.get("owner_identity"),
        "codex_home": config.get("codex_home"),
    }


def default_codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex"))


def _stored_name(state_root, thread: str, codex_home) -> Optional[str]:
    """Reuse a previously stored display name for this thread+home, even if its owner is gone."""
    path = registry_dir(state_root) / f"{thread}.json"
    if path.is_symlink() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if (isinstance(data, dict) and data.get("thread") == thread and data.get("codex_home") == str(Path(codex_home))
            and isinstance(data.get("name"), str) and data["name"]):
        return data["name"]
    return None


def discover_locked_threads(codex_home=None, state_root=None) -> list:
    """Ephemeral rows for every live Codex process holding a thread writer lock.

    Read-only: parses lock ownership from the kernel (or lsof), never writes
    locks, the state DB, or registry files. A row proves only "this process
    owns this thread right now". phase is always "unknown"; permission_class
    and model come from the state DB when a row exists, else "unknown";
    initialization_required is True while the thread has no stored row.
    """
    home = Path(codex_home) if codex_home else default_codex_home()
    lock_dir = home / "thread-writer-locks"
    rows = []
    if not lock_dir.is_dir():
        return rows
    candidates = [p for p in sorted(lock_dir.glob("*.lock")) if valid_id(p.name[: -len(".lock")])]
    # One batched probe drops the (usually many) stale lock files before any
    # per-process inspection; each survivor is still fully re-verified.
    for path in pp.locked_paths(candidates):
        thread = path.name[: -len(".lock")]
        if path.is_symlink() or not path.is_file():
            continue
        owner = pp.exclusive_lock_owner(path, comm="codex")
        if owner is None:
            continue
        meta = _thread_metadata(home, thread)
        if meta["is_child"]:
            continue
        cwd = str(owner["cwd"]) if owner.get("cwd") else meta["cwd"]
        name = (_stored_name(state_root, thread, home) if state_root is not None else None) or default_name(cwd, thread)
        rows.append({
            "version": VERSION, "kind": "codex", "thread": thread, "id": thread,
            "name": name, "cwd": cwd,
            "owner_pid": owner["pid"], "pid": owner["pid"], "owner_identity": owner["identity"],
            "exe": str(owner["exe"]) if owner.get("exe") else None, "codex_home": str(home),
            "transcript_path": None, "permission_mode": None,
            "permission_class": meta["permission_class"], "model": meta["model"],
            "phase": "unknown", "source": "writer-lock",
            "initialization_required": not meta["found"],
            "platform": pp.supported_platform()["os"],
            "registered_at": None, "updated_at": None,
            "bridge": _bridge_info(state_root, thread) if state_root is not None else None,
        })
    return rows


def sessions(state_root, *, codex_homes=None, discover: bool = True) -> SessionList:
    """Live Codex sessions, read-only, in precedence order per thread:
    hook/explicit registration > live bridge config > writer-lock discovery.

    codex_homes: extra Codex homes to scan for writer locks; the default home
    (CODEX_HOME or ~/.codex) and every home named by a registration or bridge
    config are always included. discover=False skips lock discovery.
    """
    result = SessionList()
    seen = set()
    homes = [default_codex_home()] + [Path(h) for h in (codex_homes or [])]
    # Every home ever named by a registration or bridge config is scanned, even
    # when that record's owner is gone: a custom-home session that was resumed
    # under a new pid is otherwise invisible until its first prompt.
    homes.extend(_known_homes(state_root))
    lock_rows = {}
    if discover:
        scanned = set()
        for home in homes:
            key = str(home)
            if key in scanned:
                continue
            scanned.add(key)
            for row in discover_locked_threads(home, state_root):
                lock_rows.setdefault(row["thread"], row)
    reg_dir = registry_dir(state_root)
    if reg_dir.is_dir():
        for path in sorted(reg_dir.glob("*.json")):
            record = _read_record(path)
            if record is None:
                result.skipped += 1
                continue
            if not _alive(record):
                continue
            row = dict(record)
            row["id"] = record["thread"]
            row["pid"] = record["owner_pid"]
            row["bridge"] = _bridge_info(state_root, record["thread"])
            row.setdefault("initialization_required", False)
            result.append(row)
            seen.add(record["thread"])
    root = Path(state_root)
    if root.is_dir():
        for entry in sorted(root.iterdir()):
            thread = entry.name
            if thread in seen or not valid_id(thread) or not entry.is_dir():
                continue
            bridge = _bridge_info(state_root, thread)
            if not bridge or not isinstance(bridge.get("owner_pid"), int):
                continue
            if pp.process_identity(bridge["owner_pid"]) != bridge.get("owner_identity"):
                continue
            lock_row = lock_rows.get(thread)
            if lock_row and lock_row["owner_identity"] == bridge["owner_identity"]:
                # The writer lock is the richer truth for the same process:
                # real name/cwd/exe and whether the thread is initialised.
                merged = dict(lock_row)
                merged["bridge"] = bridge
                result.append(merged)
            else:
                result.append({
                    "version": VERSION, "kind": "codex", "thread": thread, "id": thread,
                    "name": (_stored_name(state_root, thread, bridge.get("codex_home") or "")
                             or default_name(None, thread)), "cwd": None,
                    "owner_pid": bridge["owner_pid"], "pid": bridge["owner_pid"],
                    "owner_identity": bridge["owner_identity"], "exe": None,
                    "codex_home": bridge.get("codex_home"), "transcript_path": None,
                    "permission_mode": None, "permission_class": "unknown", "model": None, "phase": "unknown",
                    "source": "bridge-config", "initialization_required": False,
                    "platform": pp.supported_platform()["os"],
                    "registered_at": None, "updated_at": None, "bridge": bridge,
                })
            seen.add(thread)
    for thread, row in lock_rows.items():
        if thread in seen:
            continue
        seen.add(thread)
        result.append(row)
    result.sort(key=lambda r: (r.get("updated_at") or 0), reverse=True)
    return result


def _known_homes(state_root) -> list:
    """Codex homes named by any registration or bridge config, live or stale."""
    homes = []
    reg_dir = registry_dir(state_root)
    if reg_dir.is_dir():
        for path in reg_dir.glob("*.json"):
            if path.is_symlink():
                continue
            try:
                data = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if isinstance(data, dict) and isinstance(data.get("codex_home"), str) and data["codex_home"]:
                homes.append(Path(data["codex_home"]))
    root = Path(state_root)
    if root.is_dir():
        for entry in root.iterdir():
            if not entry.is_dir() or not valid_id(entry.name):
                continue
            bridge = _bridge_info(state_root, entry.name)
            if bridge and isinstance(bridge.get("codex_home"), str) and bridge["codex_home"]:
                homes.append(Path(bridge["codex_home"]))
    return homes


def prune(state_root) -> list:
    """Delete registrations whose owner is gone or whose file is unreadable. Returns removed thread ids."""
    removed = []
    reg_dir = registry_dir(state_root)
    if not reg_dir.is_dir():
        return removed
    for path in sorted(reg_dir.glob("*.json")):
        if path.is_symlink():
            continue
        record = _read_record(path)
        if record is None or not _alive(record):
            try:
                path.unlink()
                removed.append(path.stem)
            except OSError:
                pass
    return removed


# --------------------------------------------------------------------------
# select
# --------------------------------------------------------------------------


def describe(row: dict) -> str:
    return f"{row.get('kind', '?')} {str(row.get('id', ''))[:8]} {row.get('name', '')} {row.get('cwd') or '-'} pid={row.get('pid', '?')}"


def select_session(rows, selector, kind: Optional[str] = None) -> dict:
    """Pick exactly one row by id, name, case-insensitive name, id prefix (>=8) or pid:<n>."""
    if not isinstance(selector, str) or not selector.strip():
        raise ValueError("A session selector is required; candidates: " + ("; ".join(describe(r) for r in rows) or "none"))
    selector = selector.strip()
    pool = [r for r in rows if isinstance(r, dict) and (kind is None or r.get("kind") == kind)]
    strategies = [
        lambda r: r.get("id") == selector,
        lambda r: r.get("name") == selector,
        lambda r: isinstance(r.get("name"), str) and r["name"].lower() == selector.lower(),
        lambda r: len(selector) >= 8 and isinstance(r.get("id"), str) and r["id"].startswith(selector),
        lambda r: selector.startswith("pid:") and selector[4:].isdigit() and r.get("pid") == int(selector[4:]),
    ]
    for strategy in strategies:
        matches = [r for r in pool if strategy(r)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(f"Selector {selector!r} is ambiguous; candidates: " + "; ".join(describe(r) for r in matches))
    raise ValueError(f"No live session matches {selector!r}; candidates: " + ("; ".join(describe(r) for r in pool) or "none"))


__all__ = ["register", "sessions", "prune", "select_session", "describe", "default_name", "sanitise_name",
           "discover_locked_threads", "default_codex_home",
           "registry_dir", "SessionList", "VERSION", "REGISTRATION_EVENTS", "IDLE_EVENTS", "ACTIVE_EVENTS", "PHASES"]


def routing_rejection(payload, config, *, check_environment=True):
    """Subagent hooks may name the parent session: pin the transcript too.

    Only compare metadata paths. Never read a transcript or user/tool content.
    Missing or changed metadata fails closed instead of delivering to a child.
    """
    if payload.get("session_id") != config["thread"]:
        return "session_id mismatch"
    if payload.get("agent_id") or payload.get("agent_type") or payload.get("agent_transcript_path") is not None:
        return "subagent marker"
    environment_thread = os.environ.get("CODEX_THREAD_ID")
    if check_environment and environment_thread and environment_thread != config["thread"]:
        return "CODEX_THREAD_ID mismatch"
    transcript = payload.get("transcript_path")
    if not isinstance(transcript, str) or not transcript:
        return "missing transcript_path"
    home = Path(config["codex_home"])
    with sqlite3.connect(f"file:{home / 'state_5.sqlite'}?mode=ro", uri=True, timeout=.2) as db:
        row = db.execute("SELECT rollout_path,archived FROM threads WHERE id=?", (config["thread"],)).fetchone()
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='thread_spawn_edges'").fetchone():
            if db.execute("SELECT 1 FROM thread_spawn_edges WHERE child_thread_id=? LIMIT 1", (config["thread"],)).fetchone():
                return "child thread"
    if not row:
        return "missing thread"
    if row[1]:
        return "archived thread"
    if not row[0] or Path(transcript).resolve() != Path(row[0]).resolve():
        return "transcript_path mismatch"
    return None



def remember_hook(payload, registration, state_root):
    """Retain verified lifecycle before any peer/inbox exists; never creates a bridge.

    Separate from presence so an explicit rename/register cannot erase evidence.
    Only hook execution calls this writer. No prompt or transcript body is saved.
    """
    if payload.get('hook_event_name') not in REGISTRATION_EVENTS:
        return
    try:
        if routing_rejection(payload, registration) is not None:
            return
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error):
        return  # Presence without verifiable lifecycle remains discoverable.
    record = {k: registration[k] for k in ('thread', 'codex_home', 'owner_pid', 'owner_identity')}
    record.update(at=time.time(), event=payload['hook_event_name'], turn=payload.get('turn_id'),
                  routing='matched', transcript_path=payload['transcript_path'])
    path = _private_dir(registry_dir(state_root) / 'hooks') / (record['thread'] + '.json')
    _atomic_write_json(path, record)


def seed_lifecycle(store, state_root):
    """Carry an actual earlier hook into a newly attached inbox for the same owner.

    Recheck owner and native routing metadata. Writer-lock presence, old-owner
    records and discovery phase alone can never establish readiness.
    """
    config = store.get('config') or {}
    thread = config.get('thread')
    if not valid_id(thread):
        return False
    path = registry_dir(state_root) / 'hooks' / (thread + '.json')
    try:
        if path.is_symlink() or path.parent.is_symlink() or path.stat().st_uid != os.getuid():
            return False
        record = json.loads(path.read_text())
        if not isinstance(record, dict):
            return False
        if any(record.get(k) != config.get(k) for k in ('thread', 'owner_pid', 'owner_identity', 'codex_home')):
            return False
        if pp.process_identity(config['owner_pid']) != config['owner_identity']:
            return False
        from peer_budget import LIFECYCLE_EVENTS
        import math
        at = record.get('at')
        if (record.get('routing') != 'matched' or record.get('event') not in LIFECYCLE_EVENTS
                or type(at) not in (int, float) or not math.isfinite(at) or at > time.time()):
            return False
        # The connecting caller can be a different session. Saved hook evidence
        # is already bound to its verified owner, not the caller's environment.
        payload = {'session_id': thread, 'transcript_path': record.get('transcript_path')}
        if routing_rejection(payload, config, check_environment=False) is not None:
            return False
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        return False
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        # A real hook can arrive while connect is checking the saved evidence.
        seen = store.get('hook_seen') or {}
        if isinstance(seen, dict) and seen.get('routing') == 'matched':
            previous_at = seen.get('at')
            if (seen.get('owner_identity') != config['owner_identity']
                    or (type(previous_at) in (int, float) and previous_at >= at)):
                return False
        evidence = {k: record.get(k) for k in ('at', 'event', 'turn', 'routing', 'owner_identity')}
        evidence['source'] = 'preconnection_hook'
        phase = 'idle' if record['event'] in IDLE_EVENTS else 'active'
        for key, value in (('hook_seen', evidence), ('phase', phase)):
            store.db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, json.dumps(value)))
    return True
