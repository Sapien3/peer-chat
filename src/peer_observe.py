"""Read-only observability snapshots for every saved bridge, plus human renderers.

One snapshot is the single source of truth for `status`, `sessions`, send
feedback and doctor. Nothing here creates a Store, mutates a database, reads
message bodies, or opens a socket. Every bridge database is inspected on its
own: a corrupt, symlinked or foreign-owned file yields a row that says so and
never stops the others.
"""
from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import stat
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Optional

import peer_platform as pp

DB_TIMEOUT = 0.2
META_KEYS = ("config", "runtime", "delivery", "remaining", "wake_remaining", "budget_limit", "phase",
             "hook_seen", "hook_rejected", "stop", "worker_error", "restore_error", "wake_pending", "last_wake", "default_peer", "watch")
NON_CONSUMED = ("received", "queued", "hook_offered", "held", "forwarding", "queue_failed", "queue_uncertain")
REACHABLE_STATES = ("active_hooks", "idle_wake_enabled")  # idle_live_only cannot wake the model
RUNTIME_STATES = ("running", "listener_down", "owner_offline", "stopped", "unconfigured", "unreadable")
# Whole ANSI CSI/OSC sequences first, then any remaining control byte.
_CONTROL = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b.|[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


def valid_id(value) -> bool:
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value.lower()
    except ValueError:
        return False


class _MetaView:
    """Read-only adapter with the Store.get signature that peer_budget expects."""

    def __init__(self, meta: dict):
        self._meta = meta

    def get(self, key, default=None):
        value = self._meta.get(key)
        return default if value is None else value


def _delivery_state(meta: dict) -> str:
    try:
        from peer_budget import delivery_state
    except ImportError:  # pragma: no cover - budget module absent in a partial install
        return "unknown"
    try:
        return delivery_state(_MetaView(meta))
    except Exception:
        return "unknown"


def _read_db(path: Path) -> tuple:
    """(meta, pending_by_status, outgoing_uncertain, error) with no writes."""
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=DB_TIMEOUT)
    except sqlite3.Error as exc:
        return {}, {}, 0, f"unreadable: {type(exc).__name__}"
    meta, pending, uncertain = {}, {}, 0
    try:
        for key in META_KEYS:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            if row and row[0] is not None:
                try:
                    meta[key] = json.loads(row[0])
                except (TypeError, ValueError):
                    meta[key] = None
        failures = []
        for key, value in conn.execute("SELECT key,value FROM meta WHERE key LIKE 'notice:%'"):
            try:
                notice = json.loads(value)
            except (TypeError, ValueError):
                continue
            if isinstance(notice, dict) and str(notice.get('result', '')).startswith('unconfirmed:'):
                failures.append({'peer': key[len('notice:'):], 'at': notice.get('at'), 'result': notice['result']})
        meta['_notice_failures'] = failures
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "messages" in tables:
            for status, count in conn.execute("SELECT status, count(*) FROM messages WHERE kind='message' GROUP BY status"):
                pending[status] = count
            meta['_pending_since'] = dict(conn.execute(
                "SELECT status,min(created) FROM messages WHERE kind='message' AND status!='consumed' GROUP BY status"))
            timing = {'available': 'message_timing' in tables, 'scope': 'observed transitions since timing instrumentation',
                      'received_to_hook_seconds': None, 'received_to_ack_seconds': None,
                      'received_to_ack_after_observed_hook_seconds': None,
                      'received_to_ack_without_observed_hook_seconds': None}
            if timing['available']:
                for stage, field, filter_sql in (
                        ('hook_offered', 'received_to_hook_seconds', ''),
                        ('consumed', 'received_to_ack_seconds', ''),
                        ('consumed', 'received_to_ack_after_observed_hook_seconds',
                         " AND EXISTS (SELECT 1 FROM message_timing h WHERE h.peer=t.peer AND h.id=t.id AND h.stage='hook_offered' AND h.at<=t.at)"),
                        ('consumed', 'received_to_ack_without_observed_hook_seconds',
                         " AND NOT EXISTS (SELECT 1 FROM message_timing h WHERE h.peer=t.peer AND h.id=t.id AND h.stage='hook_offered' AND h.at<=t.at)")):
                    count, mean, maximum = conn.execute(
                        "SELECT count(*),avg(t.at-m.created),max(t.at-m.created) FROM message_timing t "
                        "JOIN messages m ON m.peer=t.peer AND m.id=t.id AND m.kind='message' "
                        "WHERE t.stage=? AND t.at>=m.created" + filter_sql, (stage,)).fetchone()
                    timing[field] = {'samples': count, 'mean': mean, 'max': maximum}
            meta['_timing'] = timing
        if "outgoing" in tables:
            row = conn.execute("SELECT count(*) FROM outgoing WHERE status IN ('uncertain','pending','writing')").fetchone()
            uncertain = int(row[0]) if row else 0
    except sqlite3.Error as exc:
        return {}, {}, 0, f"corrupt or busy: {type(exc).__name__}"
    finally:
        conn.close()
    return meta, pending, uncertain, None


def _registry_name(state_root: Path, thread: str) -> Optional[str]:
    path = state_root / "registry" / f"{thread}.json"
    try:
        if path.is_symlink() or not path.is_file():
            return None
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    name = data.get("name") if isinstance(data, dict) else None
    return name if isinstance(name, str) and name else None


def _alive(pid, identity) -> bool:
    return isinstance(pid, int) and not isinstance(pid, bool) and isinstance(identity, str) and pp.process_identity(pid) == identity


def _socket_ok(path) -> bool:
    """The listener's endpoint exists, is a socket and is ours. No connect probe."""
    try:
        info = Path(path).lstat()
    except (OSError, TypeError):
        return False
    return stat.S_ISSOCK(info.st_mode) and info.st_uid == os.getuid()


def _runtime_state(meta: dict) -> tuple:
    """(runtime_state, listener_alive). Precedence: unconfigured > stopped (explicit)
    > owner_offline (a supervised listener may deliberately stay alive to retain
    messages and send advisories) > running (live pid AND healthy socket) > listener_down."""
    config, runtime = meta.get("config"), meta.get("runtime")
    if not isinstance(config, dict):
        return "unconfigured", False
    listener_alive = isinstance(runtime, dict) and _alive(runtime.get("pid"), runtime.get("identity"))
    if meta.get("stop") is True:
        return "stopped", listener_alive
    if not _alive(config.get("owner_pid"), config.get("owner_identity")):
        return "owner_offline", listener_alive
    if listener_alive and _socket_ok(config.get("socket")):
        return "running", True
    return "listener_down", listener_alive


def key_short(key) -> str:
    """Display-only short form that keeps the DISTINGUISHING part of a key.
    codex:<home-hash>:<uuid> -> codex:<uuid[:8]>; boot:pid:start identities -> pid:start."""
    text = "" if key is None else str(key)
    if text.startswith("codex:"):
        parts = text.split(":")
        return "codex:" + (parts[-1][:8] if len(parts) >= 3 else text[6:14])
    parts = text.split(":")
    if len(parts) >= 3:
        return ":".join(parts[-2:])
    return text[:20]


def _peers(config) -> list:
    try:
        from peer_peers import all_peers
        rows = all_peers(config)
    except Exception:
        return []
    return [{"kind": r.get("kind"), "name": r.get("name"), "key": str(r.get("key", "")), "key_short": key_short(r.get("key"))}
            for r in rows.values()]


def _warning(row: dict) -> Optional[str]:
    thread = row["thread"]
    rs, ds = row["runtime_state"], row["delivery_state"]
    if rs == "unreadable":
        return f"Bridge state {row.get('error') or 'unreadable'}; run `peer-chat doctor` and inspect {thread[:8]}"
    if rs == "unconfigured":
        return "Bridge has no configuration yet; connect from either agent to set it up"
    if rs == "stopped":
        return f"Bridge stopped explicitly; nothing is delivered until `peer-chat --thread {thread} start` (the watchdog respects the stop)"
    if rs == "owner_offline":
        kept = "the listener is retaining messages" if row.get("listener_alive") else "messages are retained in the inbox"
        return f"Owning Codex process is gone; {kept}. Resume the thread in Codex; the watchdog reconnects the same verified thread automatically"
    if rs == "listener_down":
        why = "socket is missing or not ours" if row.get("listener_alive") else "listener process is not running"
        return f"Listener down ({why}); messages cannot be received until the watchdog restarts it or you run `peer-chat --thread {thread} restart`"
    if row.get("worker_error"):
        return f"Dispatcher stopped ({row['worker_error']}); the watchdog restarts it, or run `peer-chat --thread {thread} restart`"
    if row.get("restore_error"):
        watch = row.get('watch') or {}
        return f"Automatic restore failed: {row['restore_error']} (attempts {watch.get('attempts', '?')}; retry at {watch.get('next_at', '?')})"
    if ds == "manual_inbox":
        return "Inbox mode: nothing reaches the model until `peer-chat read --ack` (or set `peer-chat delivery auto`)"
    if ds == "paused_budget":
        limit = row.get("budget_limit")
        hint = f"; the owner's next prompt renews it (limit {limit})" if isinstance(limit, int) and limit > 0 else "; configure `peer-chat delivery auto --budget N`"
        return "Delivery budget exhausted: messages stay in the inbox" + hint
    if ds == "awaiting_lifecycle_hook":
        return ("Verified hook readiness is not available for this Codex process, so messages wait in the inbox: run peer-chat-setup once if never done, "
                "resume the thread if the process predates setup, or type a first prompt in a new tab; the watchdog reconnects automatically")
    if ds == "paused_wake_budget":
        return "Wake budget exhausted: an idle Codex will not be woken until the owner's next prompt renews it"
    if ds == "idle_live_only":
        return "Live mode cannot wake an idle model: messages wait for its next tool call or prompt (set `peer-chat delivery auto` to enable wake)"
    if ds == "after_turn_queue":
        return "Queue mode: delivery happens only after the current turn ends"
    if row.get("outgoing_uncertain"):
        return f"{row['outgoing_uncertain']} outgoing write(s) uncertain; check `peer-chat status` before resending"
    if row.get('runtime_state') == 'running' and row.get('supervision_enabled') is False:
        return 'Automatic recovery is not enabled for this bridge; reconnect by name or run peer-chat watch start.'
    return None


def snapshot(state_root, thread) -> Optional[dict]:
    """One bridge, read-only. None when no bridge database exists for the thread."""
    root = Path(state_root)
    if not valid_id(thread):
        return None
    bridge_dir = root / thread
    db_path = bridge_dir / "inbox.sqlite"
    try:
        if not db_path.exists() and not db_path.is_symlink():
            return None
        dir_info = bridge_dir.lstat()
        info = db_path.lstat()
    except OSError:
        return None
    row = {"thread": thread, "name": _registry_name(root, thread) or thread[:8], "runtime_state": "unreadable",
           "delivery_state": "unknown", "delivery_configured": None, "reachable": False,
           "pending_count": 0, "pending_by_status": {}, "outgoing_uncertain": 0,
           "oldest_pending_age_s": None, "pending_age_s_by_status": {}, "delivery_timing": None,
           "notice_failures": [],
           "received_total": None, "acknowledged_total": None,
           "remaining": None, "wake_remaining": None, "wake_remaining_effective": None, "wake_remaining_source": None,
           "budget_limit": None, "phase": "unknown", "evidence_stale": False, "orphan": False,
           "hook_seen": None, "hook_rejected": None, "hook_age_s": None, "peers": [], "default_peer": None, "default_peer_short": None,
           "runtime_pid": None, "owner_pid": None, "listener_alive": False, "socket_ok": False,
           "worker_error": None, "restore_error": None,
           "wake_pending": None, "error": None, "warning": None, "observed_at": time.time()}
    if stat.S_ISLNK(dir_info.st_mode):
        row["error"] = "bridge directory is a symlink"
    elif dir_info.st_uid != os.getuid():
        row["error"] = "bridge directory belongs to another user"
    elif db_path.is_symlink():
        row["error"] = "state database is a symlink"
    elif not stat.S_ISREG(info.st_mode):
        row["error"] = "state database is not a regular file"
    elif info.st_uid != os.getuid():
        row["error"] = "state database belongs to another user"
    if row["error"]:
        row["warning"] = _warning(row)
        return row
    meta, pending, uncertain, error = _read_db(db_path)
    if error:
        row["error"] = error
        row["warning"] = _warning(row)
        return row
    config = meta.get("config") if isinstance(meta.get("config"), dict) else None
    runtime = meta.get("runtime") if isinstance(meta.get("runtime"), dict) else None
    row["runtime_state"], row["listener_alive"] = _runtime_state(meta)
    row["socket_ok"] = bool(config) and _socket_ok(config.get("socket"))
    row["delivery_configured"] = _delivery_state(meta)
    row["delivery_state"] = row["delivery_configured"] if row["runtime_state"] == "running" else row["runtime_state"]
    row["pending_by_status"] = {k: v for k, v in pending.items() if k != "consumed"}
    row["pending_count"] = sum(v for k, v in pending.items() if k in NON_CONSUMED or k not in ("consumed",))
    row['received_total'] = sum(pending.values())
    row['acknowledged_total'] = pending.get('consumed', 0)
    row['delivery_timing'] = meta.get('_timing')
    row['notice_failures'] = meta.get('_notice_failures', [])
    row['pending_age_s_by_status'] = {
        status: max(0, int(row['observed_at'] - at))
        for status, at in meta.get('_pending_since', {}).items()
        if isinstance(at, (int, float)) and math.isfinite(at)}
    row['oldest_pending_age_s'] = max(row['pending_age_s_by_status'].values(), default=None)
    row["outgoing_uncertain"] = uncertain
    for key in ("remaining", "wake_remaining", "budget_limit"):
        value = meta.get(key)
        row[key] = value if value == "unlimited" or type(value) is int else None
    if row["wake_remaining"] is not None:
        row["wake_remaining_effective"], row["wake_remaining_source"] = row["wake_remaining"], "explicit"
    elif row["remaining"] is not None:
        # The runtime treats a missing wake window as "same as remaining" (legacy stores).
        row["wake_remaining_effective"], row["wake_remaining_source"] = row["remaining"], "legacy_fallback"
    phase = meta.get("phase")
    row["phase"] = phase if phase in ("idle", "active", "unknown") else "unknown"
    row["evidence_stale"] = row["runtime_state"] != "running" and (row["phase"] != "unknown" or isinstance(meta.get("hook_seen"), dict))
    row["orphan"] = row["runtime_state"] == "unconfigured" and not pending
    seen = meta.get("hook_seen")
    if isinstance(seen, dict):
        row["hook_seen"] = {k: seen.get(k) for k in ("at", "event", "turn", "routing")}
        at = seen.get("at")
        if isinstance(at, (int, float)) and not isinstance(at, bool) and math.isfinite(at):
            row["hook_age_s"] = max(0, int(row["observed_at"] - at))
    rejected = meta.get('hook_rejected')
    if isinstance(rejected, dict):
        row['hook_rejected'] = {k: rejected.get(k) for k in ('at', 'event', 'turn', 'routing')}
    watch = meta.get('watch')
    row['watch'] = {k: watch.get(k) for k in ('attempts','next_at','last_result','last_error','last_attempt','recovered_at','paused_until')} if isinstance(watch, dict) else None
    row["worker_error"] = meta.get("worker_error") if isinstance(meta.get("worker_error"), str) else None
    row["restore_error"] = meta.get("restore_error") if isinstance(meta.get("restore_error"), str) else None
    wp = meta.get("wake_pending")
    row["wake_pending"] = {"id": wp.get("id"), "status": wp.get("status")} if isinstance(wp, dict) else None
    if config:
        row['supervision_enabled'] = config.get('supervision') is True
        row["peers"] = _peers(config)
        if config.get("default_peer"):
            row["default_peer"] = str(config["default_peer"])
            row["default_peer_short"] = key_short(config["default_peer"])
        row["owner_pid"] = config.get("owner_pid") if isinstance(config.get("owner_pid"), int) else None
    if runtime:
        row["runtime_pid"] = runtime.get("pid") if isinstance(runtime.get("pid"), int) else None
    row["reachable"] = row["runtime_state"] == "running" and row["delivery_state"] in REACHABLE_STATES
    row["warning"] = _warning(row)
    if row['pending_by_status'].get('held'):
        held_note = f"{row['pending_by_status']['held']} message(s) held by a routing guard; inspect status --messages for the hold reason."
        row['warning'] = (row['warning'] + ' ' if row['warning'] else '') + held_note
    if row['notice_failures']:
        note = f"{len(row['notice_failures'])} peer advisory attempt(s) unconfirmed; no automatic resend. Inspect notice_failures in status --json."
        # Two independent warnings must read as two sentences, not run together.
        if row['warning']:
            row['warning'] = row['warning'].rstrip()
            row['warning'] += ('' if row['warning'].endswith(('.', '!', '?')) else '.') + ' ' + note
        else:
            row['warning'] = note
    return row


def snapshots(state_root) -> list:
    """Every saved bridge under state_root, dead listeners included; each independently."""
    root = Path(state_root)
    rows = []
    try:
        entries = sorted(root.iterdir())
    except OSError:
        return rows
    for entry in entries:
        if not valid_id(entry.name):
            continue
        try:
            if entry.is_symlink() or not entry.is_dir():
                continue
        except OSError:
            continue
        row = snapshot(root, entry.name)
        if row is not None:
            rows.append(row)
    order = {"running": 0, "listener_down": 1, "owner_offline": 2, "stopped": 3, "unconfigured": 4, "unreadable": 5}
    rows.sort(key=lambda r: (order.get(r["runtime_state"], 9), r["name"].lower(), r["thread"]))
    return rows


def message_status(state_root, thread, message_id=None):
    """Body-free recipient records for senders; inspection never acknowledges.

    Recent rows are bounded; an exact ID can locate older records. A missing
    timing row or unmatched last wake means unknown, not that it never happened.
    """
    observed = snapshot(state_root, thread)
    if observed is None or observed.get('error'):
        raise ValueError('No readable saved bridge for this thread')
    if message_id is not None and not valid_id(message_id):
        raise ValueError('An exact message UUID is required')
    path = Path(state_root) / thread / 'inbox.sqlite'
    with closing(sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=DB_TIMEOUT)) as db:
        where, params = (" AND id=?", (message_id,)) if message_id else ('', ())
        records = db.execute("SELECT peer,id,status,created,detail FROM messages WHERE kind='message'" + where
                             + " ORDER BY created DESC,peer LIMIT 21", params).fetchall()
        has_timing = db.execute("SELECT 1 FROM sqlite_master WHERE name='message_timing'").fetchone()
        wake_row = db.execute("SELECT value FROM meta WHERE key='last_wake'").fetchone()
        wake = json.loads(wake_row[0]) if wake_row else {}
        names = {p['key']: p.get('name') for p in observed['peers']}
        results = []
        for peer, mid, status, created, detail in records[:20]:
            timing = dict(db.execute('SELECT stage,at FROM message_timing WHERE peer=? AND id=?', (peer, mid))) if has_timing else {}
            matched_wake = (isinstance(wake, dict) and wake.get('id') == mid
                            and db.execute("SELECT count(*) FROM messages WHERE kind='message' AND id=?", (mid,)).fetchone()[0] == 1)
            results.append({'peer': peer, 'peer_short': key_short(peer), 'peer_name': names.get(peer), 'id': mid, 'status': status,
                'hold_reason': ({'Hop limit reached': 'hop_limit', 'Hop limit reached; available through read': 'hop_limit',
                                 'Peer no longer enrolled': 'peer_not_enrolled', 'Message belongs to a previously enrolled peer': 'peer_not_enrolled'}.get(detail, 'held') if status == 'held' else None),
                'acknowledged': status == 'consumed', 'created_at': created,
                'age_s': max(0, int(time.time() - created)),
                'hook_offered_at': timing.get('hook_offered'), 'acknowledged_at': timing.get('consumed'),
                'last_wake_state': wake.get('state') if matched_wake else None})
        return {'messages': results, 'more': len(records) > 20,
                'note': 'Recipient records only. Consumption is explicit acknowledgement, not task completion. Missing times/wake evidence are unknown.'}


# --------------------------------------------------------------------------
# human rendering
# --------------------------------------------------------------------------


def sanitize(value, width: int = 0, keep_tail: int = 0) -> str:
    """Printable, single-line, control- and escape-free; truncated with an ellipsis.
    keep_tail keeps that many trailing characters so a distinguishing suffix survives."""
    text = "" if value is None else str(value)
    text = _CONTROL.sub("?", text).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = "".join(ch if ch.isprintable() else "?" for ch in text)
    if width and len(text) > width:
        if keep_tail and width > keep_tail + 2:
            text = text[: width - keep_tail - 1] + "…" + text[-keep_tail:]
        else:
            text = text[: max(1, width - 1)] + "…"
    return text


def _budget_cell(row: dict) -> str:
    rem, lim = row.get("remaining"), row.get("budget_limit")
    wake, source = row.get("wake_remaining_effective"), row.get("wake_remaining_source")
    base = "-" if rem is None else (f"{rem}/{lim}" if isinstance(lim, int) and rem != "unlimited" else str(rem))
    if wake is None or (wake == rem and source != "legacy_fallback"):
        return base
    return f"{base} w{wake}" + ("~" if source == "legacy_fallback" else "")


def _phase_cell(row: dict) -> str:
    return row["phase"] + ("?" if row.get("evidence_stale") else "")


def _hook_cell(row: dict) -> str:
    seen = row.get("hook_seen")
    if not seen:
        return "none"
    age = row.get("hook_age_s")
    age_text = "?" if age is None else (f"{age}s" if age < 90 else f"{age // 60}m" if age < 5400 else f"{age // 3600}h")
    routed = "ok" if seen.get("routing") == "matched" else "rej"
    return f"{seen.get('event') or '?'} {age_text} {routed}"


def _pending_age_cell(row: dict) -> str:
    age = row.get('oldest_pending_age_s')
    if age is None:
        return '-'
    return f'{age}s' if age < 90 else f'{age // 60}m' if age < 5400 else f'{age // 3600}h'


def _table(headers, rows, widths, tails=None) -> str:
    tails = tails or [0] * len(widths)
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    for cells in rows:
        lines.append("  ".join(sanitize(c, w, t).ljust(w) for c, w, t in zip(cells, widths, tails)).rstrip())
    return "\n".join(lines)


def render_status(rows, *, show_orphans: bool = False) -> str:
    """Bridges: NAME THREAD RUNTIME DELIVERY PENDING BUDGET PHASE HOOK, then warnings.
    Orphan state directories (no config, no messages) are counted in a footer unless show_orphans."""
    if not rows:
        return "No bridges saved."
    shown = [r for r in rows if show_orphans or not r.get("orphan")]
    hidden = [r for r in rows if r.get("orphan")] if not show_orphans else []
    body = []
    for r in shown:
        body.append([r["name"], r["thread"][:8], r["runtime_state"], r["delivery_state"], str(r["pending_count"]),
                     _pending_age_cell(r), _budget_cell(r), _phase_cell(r), _hook_cell(r)])
    text = _table(["NAME", "THREAD", "RUNTIME", "DELIVERY", "PENDING", "OLDEST", "BUDGET", "PHASE", "HOOK"], body,
                  [28, 8, 13, 25, 7, 7, 11, 8, 20], tails=[9, 0, 0, 0, 0, 0, 0, 0, 0]) if shown else "No bridges saved."
    warnings = [f"! {sanitize(r['name'], 28, 9)} ({r['thread'][:8]}): {sanitize(r['warning'], 1000)}" for r in shown if r.get("warning")]
    footer = []
    if any(r.get("evidence_stale") for r in shown):
        footer.append("? = last known phase/hook from before the listener stopped")
    if any(r.get("wake_remaining_source") == "legacy_fallback" for r in shown):
        footer.append("~ = wake allowance follows the delivery budget (no separate wake window configured)")
    if hidden:
        footer.append(f"{len(hidden)} orphan state director{'y' if len(hidden) == 1 else 'ies'} hidden (no config, no messages): "
                      + ", ".join(sanitize(r["thread"][:8]) for r in hidden) + "; run with --all to list")
    return text + ("\n" + "\n".join(warnings) if warnings else "") + ("\n" + "\n".join(footer) if footer else "")


def render_sessions(rows) -> str:
    """Human discovery table: NAME THREAD PID KIND/SOURCE PHASE BRIDGE REACH, one line each."""
    if not rows:
        return "No sessions found."
    body = []
    for r in rows:
        kind = r.get("kind") or "codex"
        source = r.get("source") or r.get("runtime_state") or "-"
        bridge = r.get("bridge")
        if r.get('delivery_state'):
            delivery = r['delivery_state']
        elif isinstance(bridge, dict):
            delivery = 'unknown' if bridge.get('runtime_alive') else 'listener_down'
        elif "runtime_state" in r:
            delivery = r['runtime_state']
        else:
            delivery = 'native_unconfirmed' if kind == 'claude' else 'not_connected'
        ident = r.get("thread") or r.get("id") or ""
        init = " (needs first prompt)" if r.get("initialization_required") else ""
        body.append([r.get("name"), str(ident)[:8], str(r.get("pid") or r.get("owner_pid") or r.get("runtime_pid") or "-"),
                     f"{kind}/{source}", str(r.get("phase") or "-") + init, delivery])
    return _table(["NAME", "THREAD", "PID", "KIND/SOURCE", "PHASE", "DELIVERY"], body, [28, 8, 8, 20, 26, 25],
                  tails=[9, 0, 0, 0, 0, 0])


__all__ = ["snapshot", "snapshots", "render_status", "render_sessions", "sanitize", "key_short",
           "NON_CONSUMED", "REACHABLE_STATES", "RUNTIME_STATES"]
