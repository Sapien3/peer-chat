"""One machine-level watchdog for saved, supervised bridges; never starts models.

Contract:
- Polls every bridge read-only (sqlite ``mode=ro``); a ``Store`` is opened only
  when an action is taken, so an idle watchdog never writes a bridge database.
- Only ever signals a *listener* process whose identity was re-verified in the
  same instant; it never signals, restarts or wakes a Codex/Claude process.
- Recovery is bounded: exponential backoff 10 s -> 300 s, at most
  ``MAX_ATTEMPTS`` per rolling ``WINDOW_S``; then it pauses until the window
  resets. Explicit stop and ``supervision`` false are always respected.
- Exits by itself after ``IDLE_EXIT_S`` with nothing supervised; ``ensure``
  restarts it on demand. Its own files are private and never symlinks.
"""
from __future__ import annotations

import fcntl
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

import peer_platform as pp

BASE_DELAY_S = 10
MAX_DELAY_S = 300
MAX_ATTEMPTS = 6
WINDOW_S = 3600
STABLE_S = 30          # a restarted listener must live this long before failures reset
IDLE_EXIT_S = 60
POLL_S = 2
LOG_CAP_BYTES = 512 * 1024
DB_TIMEOUT = 0.2
WATCH_KEY = "watch"

# Indirections so tests can capture process actions without patching global modules.
_run = subprocess.run
_popen = subprocess.Popen
_kill = os.kill


def valid_id(value) -> bool:
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value.lower()
    except ValueError:
        return False


# --------------------------------------------------------------------------
# private files
# --------------------------------------------------------------------------


def _private_path(path: Path) -> Optional[Path]:
    """Path we may read/write: absent, or a regular file owned by us and not a symlink."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return path
    except OSError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        return None
    return path


def _write_private_json(path: Path, data: dict) -> bool:
    target = _private_path(path)
    if target is None:
        return False
    fd, tmp = tempfile.mkstemp(prefix=".watch-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(data, stream, sort_keys=True)
        os.chmod(tmp, 0o600)
        os.replace(tmp, target)
        return True
    finally:
        Path(tmp).unlink(missing_ok=True)


def _read_private_json(path: Path) -> Optional[dict]:
    target = _private_path(path)
    if target is None or not target.exists():
        return None
    try:
        data = json.loads(target.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _cap_log(path: Path) -> None:
    """Keep watch.log under LOG_CAP_BYTES by rotating once to watch.log.1."""
    target = _private_path(path)
    if target is None:
        return
    try:
        if target.exists() and target.stat().st_size > LOG_CAP_BYTES:
            rotated = target.with_suffix(".log.1")
            if _private_path(rotated) is not None:
                os.replace(target, rotated)
    except OSError:
        pass


def _open_log(root: Path):
    _cap_log(root / "watch.log")
    target = _private_path(root / "watch.log")
    if target is None:
        return open(os.devnull, "ab")
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    return os.fdopen(fd, "ab")


# --------------------------------------------------------------------------
# read-only inspection
# --------------------------------------------------------------------------


def _read_bridge(db_path: Path) -> Optional[dict]:
    """meta keys we need, read-only; None when unreadable or not ours."""
    try:
        parent = db_path.parent.lstat()
        info = db_path.lstat()
    except OSError:
        return None
    if (stat.S_ISLNK(parent.st_mode) or stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode)
            or parent.st_uid != os.getuid() or info.st_uid != os.getuid()):
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=DB_TIMEOUT)
    except sqlite3.Error:
        return None
    meta = {}
    try:
        for key in ("config", "runtime", "stop", WATCH_KEY):
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            if row and row[0] is not None:
                try:
                    meta[key] = json.loads(row[0])
                except (TypeError, ValueError):
                    meta[key] = None
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    return meta


def _alive(pid, identity) -> bool:
    return (isinstance(pid, int) and not isinstance(pid, bool) and isinstance(identity, str)
            and pp.process_identity(pid) == identity)


def _backoff_delay(attempts: int) -> int:
    return min(MAX_DELAY_S, BASE_DELAY_S * (2 ** max(0, attempts - 1)))


def _watch_state(meta: dict, now: float) -> dict:
    state = meta.get(WATCH_KEY) if isinstance(meta.get(WATCH_KEY), dict) else {}
    window_start = state.get("window_start")
    if not isinstance(window_start, (int, float)) or now - window_start >= WINDOW_S:
        state = {**state, "window_start": now, "attempts": 0, "paused_until": None}
    state.setdefault("attempts", 0)
    state.setdefault("next_at", 0)
    state.setdefault("last_error", None)
    state.setdefault("last_result", None)
    state.setdefault("last_attempt", None)
    state.setdefault("recovered_at", None)
    state.setdefault("paused_until", None)
    return state


def _find_replacement(root: Path, config: dict, cache: dict) -> Optional[dict]:
    """A live, verified owner of the same thread in the same Codex home, from registry/lock rows."""
    from peer_registry import sessions
    home = config.get("codex_home")
    if not isinstance(home, str) or not home:
        return None
    if home not in cache:
        try:
            cache[home] = list(sessions(root, codex_homes=[home]))
        except Exception:
            cache[home] = []
    try:
        wanted = Path(home).resolve()
    except OSError:
        return None
    for row in cache[home]:
        try:
            same = row.get("thread") == config.get("thread") and Path(row.get("codex_home", "")).resolve() == wanted
        except OSError:
            same = False
        if same and _alive(row.get("owner_pid"), row.get("owner_identity")):
            return row
    return None


# --------------------------------------------------------------------------
# actions (Store opened only here)
# --------------------------------------------------------------------------


def _with_store(db_path: Path, fn):
    from peer_chat import Store
    store = Store(db_path, timeout=DB_TIMEOUT)
    try:
        return fn(store)
    finally:
        store.close()


def _record(db_path: Path, state: dict, **updates) -> dict:
    state = {**state, **updates}

    def write(store):
        store.put(WATCH_KEY, state)
        if "restore_error" in updates:
            store.put("restore_error", updates["restore_error"])
        if "recovery_event" in updates and updates["recovery_event"]:
            store.put("recovery_event", updates["recovery_event"])
        return state
    return _with_store(db_path, write)


def _signal_listener(runtime: dict) -> bool:
    """SIGTERM the listener only; identity re-verified in the same instant."""
    pid, identity = runtime.get("pid"), runtime.get("identity")
    if not _alive(pid, identity):
        return False
    try:
        _kill(pid, signal.SIGTERM)
    except OSError:
        return False
    return True


def _budget_exhausted(state: dict, now: float) -> Optional[float]:
    """paused_until when this attempt would exceed MAX_ATTEMPTS in the window, else None."""
    if state["attempts"] + 1 > MAX_ATTEMPTS:
        return state["window_start"] + WINDOW_S
    return None


def _recover(root: Path, db_path: Path, config: dict, state: dict, owner: dict, reason: str, now: float, log) -> dict:
    """Bounded attempt to restart the listener for a verified live owner."""
    paused = _budget_exhausted(state, now)
    if paused:
        return _record(db_path, state, paused_until=paused, next_at=paused, last_result="paused")
    attempts = state["attempts"] + 1
    event = {"id": str(uuid.uuid4()), "at": now, "reason": reason, "attempt": attempts}
    if not _alive(owner.get("owner_pid"), owner.get("owner_identity")):
        return _record(db_path, state, last_attempt=now, last_result="skipped", last_error="owner changed before recovery")
    # Persist the attempt BEFORE spawning: a watchdog crash mid-attempt must not retry ambiguously.
    next_at = now + _backoff_delay(attempts)
    state = _record(db_path, state, attempts=attempts, next_at=next_at, last_attempt=now, last_result="attempting",
                    paused_until=(state["window_start"] + WINDOW_S) if attempts >= MAX_ATTEMPTS else None,
                    recovery_event=event)
    cmd = [sys.executable, "-m", "peer_chat", "--thread", config["thread"], "--state-root", str(root),
           "start", "--recover", "--owner-pid", str(owner["owner_pid"]), "--owner-identity", owner["owner_identity"]]
    try:
        result = _run(cmd, env=dict(os.environ, CODEX_HOME=str(owner.get("codex_home") or config.get("codex_home"))),
                      capture_output=True, text=True, timeout=20)
        ok, error = result.returncode == 0, (result.stderr or "")[-200:].strip() or None
    except (OSError, subprocess.SubprocessError) as exc:
        ok, error = False, type(exc).__name__
    if ok:
        # Started, not yet trusted: attempts stay counted until the listener has been
        # live for STABLE_S, so a crash loop cannot restart faster than the backoff.
        return _record(db_path, state, last_result="started", last_error=None, recovered_at=now, restore_error=None)
    if attempts >= MAX_ATTEMPTS:
        state["next_at"] = state["paused_until"]
    _log(log, f"recovery failed for {config['thread'][:8]} attempt {attempts}: {error}")
    return _record(db_path, state, last_result="failed", last_error=error or "recovery command failed",
                   restore_error="Automatic listener recovery failed; see `peer-chat status` watch fields.")


def _keep_endpoint(root: Path, db_path: Path, config: dict, state: dict, now: float, log) -> dict:
    """Owner gone, no replacement: keep an owner-offline listener so messages are retained.
    Same attempt cap, window and pre-spawn persistence as recovery."""
    paused = _budget_exhausted(state, now)
    if paused:
        return _record(db_path, state, paused_until=paused, next_at=paused, last_result="paused")
    attempts = state["attempts"] + 1
    state = _record(db_path, state, attempts=attempts, next_at=now + _backoff_delay(attempts), last_attempt=now,
                    last_result="attempting_endpoint",
                    paused_until=(state["window_start"] + WINDOW_S) if attempts >= MAX_ATTEMPTS else None)
    try:
        with (db_path.parent / "daemon.log").open("ab") as daemon_log:
            _popen([sys.executable, "-m", "peer_chat", "--thread", config["thread"], "--state-root", str(root), "_serve"],
                   stdin=subprocess.DEVNULL, stdout=daemon_log, stderr=daemon_log, close_fds=True, start_new_session=True)
    except (OSError, subprocess.SubprocessError) as exc:
        _log(log, f"endpoint keep failed for {config['thread'][:8]}: {type(exc).__name__}")
        return _record(db_path, state, last_result="failed", last_error=type(exc).__name__)
    return _record(db_path, state, last_result="endpoint_kept", recovered_at=now)


def _log(log, text: str) -> None:
    try:
        log.write((time.strftime("%Y-%m-%dT%H:%M:%S ") + text + "\n").encode())
        log.flush()
    except (OSError, ValueError, AttributeError):
        pass


# --------------------------------------------------------------------------
# public API
# --------------------------------------------------------------------------


def ensure(state_root) -> dict:
    """Start the watchdog if none is alive; returns its record or {'starting': pid}."""
    from peer_chat import private_dir
    root = private_dir(state_root)
    old = _read_private_json(root / "watch.json")
    if old and _alive(old.get("pid"), old.get("identity")):
        return old
    log = _open_log(root)
    try:
        proc = _popen([sys.executable, "-m", "peer_watch", str(root)],
                      stdin=subprocess.DEVNULL, stdout=log, stderr=log, close_fds=True, start_new_session=True)
    finally:
        log.close()
    return {"starting": proc.pid}


def tick(root, now: Optional[float] = None, log=None) -> dict:
    """One pass over every saved bridge. Returns a summary: supervised, actions, errors."""
    root = Path(root)
    now = time.time() if now is None else now
    summary = {"supervised": 0, "active": 0, "actions": [], "errors": 0}
    cache = {}
    try:
        paths = sorted(root.glob("*/inbox.sqlite"))
    except OSError:
        return summary
    for db_path in paths:
        thread = db_path.parent.name
        if not valid_id(thread):
            continue
        meta = _read_bridge(db_path)
        if meta is None:
            continue
        config = meta.get("config")
        if not isinstance(config, dict) or not config.get("supervision"):
            continue
        if meta.get("stop") is True:
            continue  # explicit stop always wins
        summary["supervised"] += 1
        try:
            runtime = meta.get("runtime") if isinstance(meta.get("runtime"), dict) else {}
            live = _alive(runtime.get("pid"), runtime.get("identity"))
            owner_live = _alive(config.get("owner_pid"), config.get("owner_identity"))
            state = _watch_state(meta, now)
            replacement = None if owner_live else _find_replacement(root, config, cache)
            if live and replacement:
                # Old transport belongs to a dead owner and a verified new owner exists:
                # ask only that listener to exit; recovery follows once it is gone.
                if _signal_listener(runtime):
                    summary["actions"].append((thread, "signal_old_listener"))
                    _record(db_path, state, last_result="signalled_old_listener", last_attempt=now)
                continue
            if live:
                summary["active"] += 1
                recovered_at = state.get("recovered_at")
                stable = not isinstance(recovered_at, (int, float)) or now - recovered_at >= STABLE_S
                if state.get("attempts") and stable and state.get("last_result") != "recovered":
                    _record(db_path, state, attempts=0, next_at=0, last_result="recovered", paused_until=None,
                            last_error=None, restore_error=None)
                continue
            if state.get("paused_until") and now < state["paused_until"]:
                continue
            if now < state.get("next_at", 0):
                continue
            if replacement:
                owner = {"owner_pid": replacement["owner_pid"], "owner_identity": replacement["owner_identity"],
                         "codex_home": replacement.get("codex_home")}
                _recover(root, db_path, config, state, owner, "owner_resumed", now, log)
                summary["actions"].append((thread, "recover_owner_resumed"))
            elif owner_live:
                owner = {"owner_pid": config["owner_pid"], "owner_identity": config["owner_identity"],
                         "codex_home": config.get("codex_home")}
                _recover(root, db_path, config, state, owner, "listener_exited", now, log)
                summary["actions"].append((thread, "recover_listener_exited"))
            else:
                _keep_endpoint(root, db_path, config, state, now, log)
                summary["actions"].append((thread, "keep_endpoint_owner_offline"))
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.SubprocessError) as exc:
            summary["errors"] += 1
            _log(log, f"tick error for {thread[:8]}: {type(exc).__name__}")
            continue
    return summary


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        print("usage: peer_watch STATE_ROOT", file=sys.stderr)
        return 2
    root = Path(argv[0])
    from peer_chat import private_dir
    private_dir(root)
    lock_path = _private_path(root / "watch.lock")
    if lock_path is None:
        return 1
    lock = open(lock_path, "a")
    os.chmod(lock_path, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return 0
    stopping = {"flag": False}

    def stop(*_):
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log = _open_log(root)
    started = time.time()
    record = {"pid": os.getpid(), "identity": pp.process_identity(os.getpid()), "started_at": started,
              "last_tick": None, "ticks": 0, "supervised": 0, "active": 0, "errors": {"count": 0, "last": None},
              "exit_reason": None}
    if not _write_private_json(root / "watch.json", record):
        return 1
    last_supervised = started
    try:
        while not stopping["flag"] and root.exists():
            summary = tick(root, log=log)
            now = time.time()
            record.update(last_tick=now, ticks=record["ticks"] + 1, supervised=summary["supervised"], active=summary["active"])
            if summary["errors"]:
                record["errors"] = {"count": record["errors"]["count"] + summary["errors"], "last": now}
            if summary["supervised"]:
                last_supervised = now
            elif now - last_supervised >= IDLE_EXIT_S:
                record["exit_reason"] = "idle: nothing supervised"
                _write_private_json(root / "watch.json", record)
                break
            _write_private_json(root / "watch.json", record)
            time.sleep(POLL_S)
        else:
            record["exit_reason"] = "stopped" if stopping["flag"] else "state root removed"
            if root.exists():
                _write_private_json(root / "watch.json", record)
    finally:
        log.close()
        lock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
