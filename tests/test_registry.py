"""Tests for peer_registry.py: presence registration, listing, selection."""

import json
import os
import sqlite3
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import peer_platform as pp  # noqa: E402
import peer_registry as reg  # noqa: E402

DISABLED = '{"type":"disabled"}'
READ_ONLY = '{"type":"read-only"}'
WORKSPACE = '{"type":"workspace-write","writable_roots":[],"network_access":false,"exclude_tmpdir_env_var":false,"exclude_slash_tmp":false}'


def make_state_db(home, threads, *, edges=(), model_column=True):
    home.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(home / "state_5.sqlite")
    cols = "id TEXT PRIMARY KEY, cwd TEXT NOT NULL DEFAULT '', sandbox_policy TEXT NOT NULL, approval_mode TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0"
    if model_column:
        cols += ", model TEXT"
    conn.execute(f"CREATE TABLE threads ({cols})")
    conn.execute("CREATE TABLE thread_spawn_edges (parent_thread_id TEXT NOT NULL, child_thread_id TEXT NOT NULL PRIMARY KEY, status TEXT NOT NULL)")
    for row in threads:
        tid, cwd, sandbox, approval, archived = row[:5]
        conn.execute("INSERT INTO threads (id, cwd, sandbox_policy, approval_mode, archived) VALUES (?,?,?,?,?)", (tid, cwd, sandbox, approval, archived))
        if model_column and len(row) > 5:
            conn.execute("UPDATE threads SET model=? WHERE id=?", (row[5], tid))
    for parent, child, status in edges:
        conn.execute("INSERT INTO thread_spawn_edges VALUES (?,?,?)", (parent, child, status))
    conn.commit()
    conn.close()


def hook_payload(thread, event="SessionStart", **extra):
    base = {"session_id": thread, "hook_event_name": event, "cwd": "/work/project-a", "model": "gpt-6-astra",
            "permission_mode": "danger-full-access", "transcript_path": f"/sessions/rollout-{thread}.jsonl"}
    base.update(extra)
    return base


def spawn_sleeper():
    child = subprocess.Popen([sys.executable, "-c", "import sys,time; sys.stdout.write('r\\n'); sys.stdout.flush(); time.sleep(30)"],
                             stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "r"
    return child


@pytest.fixture(autouse=True)
def _isolate_from_real_codex(monkeypatch, tmp_path_factory):
    # The suite may itself run under a Codex session whose exec tool exports
    # CODEX_THREAD_ID; the env-thread guard must only see what a test sets.
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    # Writer-lock discovery is real: without this, sessions() would list the
    # live Codex terminals of whoever runs the suite. Point the default home
    # at an empty directory; tests that want discovery set CODEX_HOME themselves.
    empty = tmp_path_factory.mktemp("empty-codex-home")
    monkeypatch.setenv("CODEX_HOME", str(empty))


@pytest.fixture
def state_root(tmp_path):
    return tmp_path / "state"


@pytest.fixture
def codex_home(tmp_path):
    home = tmp_path / "codex"
    make_state_db(home, [
        ("11111111-1111-4111-8111-111111111111", "/work/db-cwd", DISABLED, "never", 0, "gpt-6-astra"),
        ("22222222-2222-4222-8222-222222222222", "/work/ro", READ_ONLY, "on-request", 0, "gpt-6"),
        ("33333333-3333-4333-8333-333333333333", "/work/ro-never", READ_ONLY, "never", 0, None),
        ("44444444-4444-4444-8444-444444444444", "/work/archived", DISABLED, "never", 1, "x"),
        ("55555555-5555-4555-8555-555555555555", "/work/child", DISABLED, "never", 0, "x"),
        ("66666666-6666-4666-8666-666666666666", "/work/bad", "disabled", "never", 0, "x"),
    ], edges=[("11111111-1111-4111-8111-111111111111", "55555555-5555-4555-8555-555555555555", "open")])
    return home


T1 = "11111111-1111-4111-8111-111111111111"
T2 = "22222222-2222-4222-8222-222222222222"
T3 = "33333333-3333-4333-8333-333333333333"
T4 = "44444444-4444-4444-8444-444444444444"
T5 = "55555555-5555-4555-8555-555555555555"
T6 = "66666666-6666-4666-8666-666666666666"
ME = os.getpid()


# --------------------------------------------------------------------------
# register
# --------------------------------------------------------------------------


def test_register_hook_payload_full_record(codex_home, state_root):
    rec = reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
    assert rec is not None
    assert set(rec) == {"version", "kind", "thread", "name", "cwd", "owner_pid", "owner_identity", "exe", "codex_home",
                        "transcript_path", "permission_mode", "permission_class", "model", "phase", "source", "platform",
                        "registered_at", "updated_at"}
    assert rec["version"] == 1 and rec["kind"] == "codex" and rec["thread"] == T1
    assert rec["owner_pid"] == ME and rec["owner_identity"] == pp.process_identity(ME)
    assert rec["exe"] == str(Path(sys.executable).resolve())
    assert rec["cwd"] == "/work/project-a", "hook cwd wins over DB cwd"
    assert rec["name"] == "project-a-11111111"
    assert rec["transcript_path"].endswith(".jsonl")
    assert rec["permission_mode"] == "danger-full-access"
    assert rec["permission_class"] == "bypass"
    assert rec["model"] == "gpt-6-astra"
    assert rec["source"] == "hook:SessionStart" and rec["phase"] == "idle"
    assert rec["platform"] == pp.supported_platform()["os"]
    assert rec["registered_at"] == rec["updated_at"]
    on_disk = json.loads((state_root / "registry" / f"{T1}.json").read_text())
    assert on_disk == rec


def test_register_file_and_dir_modes_and_nothing_extra_stored(codex_home, state_root):
    payload = hook_payload(T1, tool_input={"command": "SECRET-CMD"}, prompt="SECRET-PROMPT", env={"TOKEN": "SECRET-T"})
    reg.register(payload, codex_home, state_root, owner_pid=ME)
    path = state_root / "registry" / f"{T1}.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    for d in (path.parent, state_root):
        assert stat.S_IMODE(d.stat().st_mode) == 0o700
    assert "SECRET" not in path.read_text()
    assert not list(path.parent.glob(".reg-*")), "no temp files left behind"


def test_register_explicit_payload_and_db_fallbacks(codex_home, state_root):
    rec = reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME)
    assert rec["source"] == "explicit" and rec["phase"] == "unknown"
    assert rec["cwd"] == "/work/db-cwd" and rec["model"] == "gpt-6-astra"
    assert rec["name"] == "db-cwd-11111111"
    assert rec["transcript_path"] is None and rec["permission_mode"] is None


def test_register_permission_class_never_guesses(codex_home, state_root):
    assert reg.register({"session_id": T2}, codex_home, state_root, owner_pid=ME)["permission_class"] == "prompting"
    assert reg.register({"session_id": T3}, codex_home, state_root, owner_pid=ME)["permission_class"] == "unknown"
    assert reg.register({"session_id": T4}, codex_home, state_root, owner_pid=ME)["permission_class"] == "unknown"
    assert reg.register({"session_id": T6}, codex_home, state_root, owner_pid=ME)["permission_class"] == "unknown"
    unknown_thread = str(uuid.uuid4())
    rec = reg.register(hook_payload(unknown_thread), codex_home, state_root, owner_pid=ME)
    assert rec is not None and rec["permission_class"] == "unknown"
    assert rec["cwd"] == "/work/project-a"


def test_register_without_state_db_still_registers_unknown(tmp_path, state_root):
    rec = reg.register(hook_payload(T1), tmp_path / "no-codex", state_root, owner_pid=ME)
    assert rec is not None and rec["permission_class"] == "unknown" and rec["model"] == "gpt-6-astra"


def test_register_old_schema_without_model_column(tmp_path, state_root):
    home = tmp_path / "old"
    make_state_db(home, [(T1, "/w", DISABLED, "never", 0)], model_column=False)
    rec = reg.register({"session_id": T1}, home, state_root, owner_pid=ME)
    assert rec["permission_class"] == "bypass" and rec["model"] is None


def test_register_locked_db_degrades_to_unknown(codex_home, state_root):
    lock = sqlite3.connect(codex_home / "state_5.sqlite", isolation_level=None)
    lock.execute("PRAGMA locking_mode=EXCLUSIVE")
    lock.execute("BEGIN EXCLUSIVE")
    try:
        start = time.monotonic()
        rec = reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
        assert time.monotonic() - start < 2
        assert rec is not None and rec["permission_class"] == "unknown"
    finally:
        lock.execute("COMMIT")
        lock.close()


@pytest.mark.parametrize("bad", [
    {"session_id": "not-a-uuid"},
    {"session_id": None},
    {},
    "string",
    None,
    hook_payload(T1, agent_id="a1"),
    hook_payload(T1, agent_type="explorer"),
    hook_payload(T1, agent_transcript_path="/x"),
    hook_payload(T1, agent_transcript_path=""),
    hook_payload(T1, event="SubagentStart"),
    hook_payload(T1, event="SessionEnd"),
    hook_payload(T1, event=""),
])
def test_register_rejects_invalid_and_subagent_payloads(codex_home, state_root, bad):
    assert reg.register(bad, codex_home, state_root, owner_pid=ME) is None
    assert not (state_root / "registry").exists() or not list((state_root / "registry").glob("*.json"))


def test_register_rejects_child_thread(codex_home, state_root):
    assert reg.register(hook_payload(T5), codex_home, state_root, owner_pid=ME) is None


def test_register_rejects_dead_or_invalid_owner(codex_home, state_root):
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    assert reg.register(hook_payload(T1), codex_home, state_root, owner_pid=child.pid) is None
    for pid in (0, -1, True, 2**22 - 1):
        assert reg.register(hook_payload(T1), codex_home, state_root, owner_pid=pid) is None


def test_register_owner_defaults_to_parent_walk(codex_home, state_root, monkeypatch):
    monkeypatch.setattr(pp, "find_owner_pid", lambda start: ME)
    rec = reg.register(hook_payload(T1), codex_home, state_root)
    assert rec["owner_pid"] == ME
    monkeypatch.setattr(pp, "find_owner_pid", lambda start: None)
    assert reg.register(hook_payload(T1), codex_home, state_root) is None


def test_register_refresh_preserves_registered_at_and_bumps_updated(codex_home, state_root):
    first = reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
    time.sleep(0.01)
    second = reg.register(hook_payload(T1, event="PostToolUse", cwd="/work/moved"), codex_home, state_root, owner_pid=ME)
    assert second["registered_at"] == first["registered_at"]
    assert second["updated_at"] > first["updated_at"]
    assert second["source"] == "hook:PostToolUse" and second["cwd"] == "/work/moved"
    assert len(list((state_root / "registry").glob("*.json"))) == 1


def test_register_name_explicit_and_sanitised(codex_home, state_root):
    rec = reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME, name="Scraper Run #2 / prod")
    assert rec["name"] == "Scraper-Run-2-prod"
    rec = reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME, name="x" * 200)
    assert len(rec["name"]) == 64
    rec = reg.register(hook_payload(T1, cwd=""), codex_home, state_root, owner_pid=ME)
    assert rec["name"] == "x" * 64, "a refresh without a name keeps the established one"
    rec = reg.register(hook_payload(T2, cwd=""), codex_home, state_root, owner_pid=ME)
    assert rec["name"] == "ro-22222222", "first registration without cwd falls back to the DB cwd"
    rec = reg.register(hook_payload(str(uuid.uuid4()), cwd=""), codex_home, state_root, owner_pid=ME)
    assert len(rec["name"]) == 8


def test_register_refuses_symlink_target(codex_home, state_root, tmp_path):
    reg_dir = state_root / "registry"
    reg_dir.mkdir(parents=True, mode=0o700)
    victim = tmp_path / "victim.json"
    victim.write_text("keep")
    (reg_dir / f"{T1}.json").symlink_to(victim)
    with pytest.raises(ValueError):
        reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
    assert victim.read_text() == "keep"


# --------------------------------------------------------------------------
# sessions / prune
# --------------------------------------------------------------------------


def test_sessions_lists_live_and_hides_dead(codex_home, state_root):
    reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
    sleeper = spawn_sleeper()
    try:
        reg.register(hook_payload(T2), codex_home, state_root, owner_pid=sleeper.pid)
        rows = reg.sessions(state_root)
        assert {r["id"] for r in rows} == {T1, T2}
        assert all(r["pid"] == r["owner_pid"] and r["bridge"] is None for r in rows)
    finally:
        sleeper.kill()
        sleeper.wait()
    rows = reg.sessions(state_root)
    assert [r["id"] for r in rows] == [T1]
    assert (state_root / "registry" / f"{T2}.json").exists(), "sessions() never deletes"
    assert rows.skipped == 0
    assert reg.prune(state_root) == [T2]
    assert not (state_root / "registry" / f"{T2}.json").exists()
    assert (state_root / "registry" / f"{T1}.json").exists()


def test_sessions_skips_corrupt_foreign_and_mismatched_files(codex_home, state_root):
    reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
    reg_dir = state_root / "registry"
    (reg_dir / f"{T2}.json").write_text("{not json")
    (reg_dir / f"{T3}.json").write_text(json.dumps({"version": 1, "kind": "codex", "thread": T4, "owner_pid": ME, "owner_identity": pp.process_identity(ME)}))
    (reg_dir / f"{T4}.json").write_text(json.dumps({"version": 99, "kind": "codex", "thread": T4, "owner_pid": ME, "owner_identity": pp.process_identity(ME)}))
    (reg_dir / "notes.json").write_text(json.dumps({"version": 1, "kind": "codex", "thread": T5, "owner_pid": ME, "owner_identity": pp.process_identity(ME)}))
    rows = reg.sessions(state_root)
    assert [r["id"] for r in rows] == [T1]
    assert rows.skipped == 4
    assert set(reg.prune(state_root)) == {T2, T3, T4, "notes"}


def test_sessions_empty_when_no_state(tmp_path):
    rows = reg.sessions(tmp_path / "nothing")
    assert rows == [] and rows.skipped == 0
    assert reg.prune(tmp_path / "nothing") == []


def make_bridge(state_root, thread, *, owner_pid, runtime_pid=None, delivery="live", socket="/run/user/1/peer-chat/x.sock"):
    d = state_root / thread
    d.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(d / "inbox.sqlite")
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    config = {"thread": thread, "owner_pid": owner_pid, "owner_identity": pp.process_identity(owner_pid), "socket": socket,
              "codex_home": "/h", "peer": {"pid": 4242, "identity": "b:4242:1", "socket": "/c.sock", "name": "Fable"}}
    conn.execute("INSERT INTO meta VALUES ('config', ?)", (json.dumps(config),))
    conn.execute("INSERT INTO meta VALUES ('delivery', ?)", (json.dumps(delivery),))
    if runtime_pid is not None:
        conn.execute("INSERT INTO meta VALUES ('runtime', ?)", (json.dumps({"pid": runtime_pid, "identity": pp.process_identity(runtime_pid), "socket": socket}),))
    conn.commit()
    conn.close()


def test_sessions_attach_bridge_info_and_list_bridge_only_threads(codex_home, state_root):
    reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME)
    make_bridge(state_root, T1, owner_pid=ME, runtime_pid=ME)
    make_bridge(state_root, T2, owner_pid=ME)  # bridge config without registration, owner alive
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    (state_root / T3).mkdir()
    conn = sqlite3.connect(state_root / T3 / "inbox.sqlite")
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO meta VALUES ('config', ?)", (json.dumps({"thread": T3, "owner_pid": dead.pid, "owner_identity": "gone", "socket": "/s"}),))
    conn.commit()
    conn.close()
    rows = {r["id"]: r for r in reg.sessions(state_root)}
    assert set(rows) == {T1, T2}
    b1 = rows[T1]["bridge"]
    assert b1["socket"] == "/run/user/1/peer-chat/x.sock" and b1["peer_pid"] == 4242 and b1["peer_name"] == "Fable"
    assert b1["delivery"] == "live" and b1["runtime_alive"] is True
    assert rows[T1]["source"] == "hook:SessionStart"
    b2 = rows[T2]
    assert b2["source"] == "bridge-config" and b2["bridge"]["runtime_alive"] is False
    assert b2["permission_class"] == "unknown" and b2["name"] == T2[:8]


def test_sessions_bridge_dir_without_db_is_ignored(state_root):
    (state_root / T1).mkdir(parents=True)
    (state_root / "registry").mkdir()
    (state_root / "archive").mkdir()
    (state_root / "notes.txt").write_text("x")
    assert reg.sessions(state_root) == []


# --------------------------------------------------------------------------
# select_session
# --------------------------------------------------------------------------


def rows_fixture():
    return [
        {"kind": "codex", "id": T1, "name": "project-a-11111111", "cwd": "/work/a", "pid": 100},
        {"kind": "codex", "id": T2, "name": "Scraper", "cwd": "/work/b", "pid": 200},
        {"kind": "codex", "id": T3, "name": "scraper", "cwd": "/work/c", "pid": 300},
        {"kind": "claude", "id": "s-claude-1", "name": "Fable", "cwd": "/work/a", "pid": 400},
        {"kind": "claude", "id": "s-claude-2", "name": "project-a-11111111", "cwd": "/work/z", "pid": 500},
    ]


def test_select_exact_id_and_prefix():
    rows = rows_fixture()
    assert reg.select_session(rows, T1)["pid"] == 100
    assert reg.select_session(rows, T2[:8])["pid"] == 200
    assert reg.select_session(rows, "s-claude-1")["pid"] == 400
    with pytest.raises(ValueError):
        reg.select_session(rows, T2[:7])  # prefix too short


def test_select_name_exact_then_case_insensitive():
    rows = rows_fixture()
    assert reg.select_session(rows, "Scraper")["pid"] == 200
    assert reg.select_session(rows, "scraper")["pid"] == 300
    with pytest.raises(ValueError, match="ambiguous"):
        reg.select_session(rows, "SCRAPER")
    assert reg.select_session(rows, "Fable")["kind"] == "claude"


def test_select_kind_filter_resolves_cross_kind_name_clash():
    rows = rows_fixture()
    with pytest.raises(ValueError, match="ambiguous") as err:
        reg.select_session(rows, "project-a-11111111")
    assert "codex 11111111" in str(err.value) and "claude s-claude" in str(err.value)
    assert reg.select_session(rows, "project-a-11111111", kind="codex")["pid"] == 100
    assert reg.select_session(rows, "project-a-11111111", kind="claude")["pid"] == 500
    with pytest.raises(ValueError, match="No live session"):
        reg.select_session(rows, "Fable", kind="codex")


def test_select_pid_and_no_match_lists_candidates():
    rows = rows_fixture()
    assert reg.select_session(rows, "pid:300")["id"] == T3
    with pytest.raises(ValueError) as err:
        reg.select_session(rows, "nothing-here")
    msg = str(err.value)
    assert "No live session" in msg and "Scraper" in msg and "/work/b" in msg and "pid=200" in msg
    with pytest.raises(ValueError):
        reg.select_session(rows, "pid:abc")
    with pytest.raises(ValueError, match="candidates: none"):
        reg.select_session([], "anything")
    for empty in ("", "   ", None):
        with pytest.raises(ValueError):
            reg.select_session(rows, empty)


def test_select_never_fuzzy_matches():
    rows = rows_fixture()
    for sel in ("Scrap", "scraper-", "project", "/work/a", "100"):
        with pytest.raises(ValueError):
            reg.select_session(rows, sel)


def test_select_on_real_sessions(codex_home, state_root):
    reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME, name="Astra")
    rows = reg.sessions(state_root)
    picked = reg.select_session(rows, "astra", kind="codex")
    assert picked["id"] == T1 and picked["owner_identity"] == pp.process_identity(ME)
    assert reg.select_session(rows, f"pid:{ME}")["id"] == T1


# --------------------------------------------------------------------------
# phase, env thread guard, transcript metadata guard
# --------------------------------------------------------------------------


@pytest.mark.parametrize("event,phase", [("SessionStart", "idle"), ("Stop", "idle"), ("UserPromptSubmit", "active"), ("PostToolUse", "active")])
def test_register_phase_from_hook_event(codex_home, state_root, event, phase):
    rec = reg.register(hook_payload(T1, event=event), codex_home, state_root, owner_pid=ME, phase="active" if phase == "idle" else "idle")
    assert rec["phase"] == phase, "hook events decide the phase, the caller argument is ignored"
    assert rec["source"] == f"hook:{event}"


def test_register_explicit_phase_only_when_trusted_value(codex_home, state_root):
    assert reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME, phase="idle")["phase"] == "idle"
    assert reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME, phase="active")["phase"] == "active"
    assert reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME, phase="busy")["phase"] == "unknown"
    assert reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME)["phase"] == "unknown"


def test_register_env_thread_mismatch_rejected(codex_home, state_root, monkeypatch):
    monkeypatch.setenv("CODEX_THREAD_ID", T2)
    assert reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME) is None
    monkeypatch.setenv("CODEX_THREAD_ID", T1)
    assert reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME) is not None
    monkeypatch.delenv("CODEX_THREAD_ID")
    assert reg.register(hook_payload(T1), codex_home, state_root, owner_pid=ME) is not None


def test_register_transcript_must_match_db_rollout_when_both_known(tmp_path, state_root):
    home = tmp_path / "codex-rollout"
    home.mkdir()
    conn = sqlite3.connect(home / "state_5.sqlite")
    conn.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, cwd TEXT, rollout_path TEXT, sandbox_policy TEXT, approval_mode TEXT, archived INTEGER DEFAULT 0)")
    conn.execute("INSERT INTO threads VALUES (?,?,?,?,?,0)", (T1, "/w", str(tmp_path / "rollout-T1.jsonl"), DISABLED, "never"))
    conn.commit(); conn.close()
    good = hook_payload(T1, transcript_path=str(tmp_path / "rollout-T1.jsonl"))
    assert reg.register(good, home, state_root, owner_pid=ME) is not None
    bad = hook_payload(T1, transcript_path=str(tmp_path / "rollout-child.jsonl"))
    assert reg.register(bad, home, state_root, owner_pid=ME) is None
    # No transcript in the payload, or no rollout in the DB: no comparison, still registers.
    assert reg.register({"session_id": T1}, home, state_root, owner_pid=ME) is not None
    assert reg.register(hook_payload(str(uuid.uuid4())), home, state_root, owner_pid=ME) is not None


# --------------------------------------------------------------------------
# name stability across refreshes (Claude connects by name)
# --------------------------------------------------------------------------


def test_explicit_name_survives_hook_refreshes_and_rename_still_works(codex_home, state_root):
    first = reg.register({"session_id": T1, "cwd": "/work/example-project"}, codex_home, state_root, owner_pid=ME, name="astra-peer-chat")
    assert first["name"] == "astra-peer-chat"
    for event in ("SessionStart", "UserPromptSubmit", "PostToolUse", "Stop"):
        rec = reg.register(hook_payload(T1, event=event, cwd="/work/example-project"), codex_home, state_root, owner_pid=ME)
        assert rec["name"] == "astra-peer-chat", event
    rec = reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME)
    assert rec["name"] == "astra-peer-chat", "explicit re-registration without a name keeps it too"
    rec = reg.register({"session_id": T1}, codex_home, state_root, owner_pid=ME, name="scraper-run")
    assert rec["name"] == "scraper-run"
    rec = reg.register(hook_payload(T1, event="PostToolUse"), codex_home, state_root, owner_pid=ME)
    assert rec["name"] == "scraper-run"
    rows = reg.sessions(state_root)
    assert reg.select_session(rows, "scraper-run", kind="codex")["id"] == T1


def test_name_follows_thread_across_resume_with_new_owner(codex_home, state_root):
    sleeper = spawn_sleeper()
    try:
        old = reg.register({"session_id": T1, "cwd": "/w/old"}, codex_home, state_root, owner_pid=sleeper.pid, name="astra-life")
        assert old["name"] == "astra-life"
        old_identity = old["owner_identity"]
    finally:
        sleeper.kill()
        sleeper.wait()
    resumed = reg.register(hook_payload(T1, cwd="/w/new"), codex_home, state_root, owner_pid=ME)
    assert resumed["name"] == "astra-life", "display name follows the thread UUID across resume"
    assert resumed["owner_identity"] == pp.process_identity(ME) and resumed["owner_identity"] != old_identity
    assert resumed["owner_pid"] == ME
    assert resumed["registered_at"] == old["registered_at"]
    rows = reg.sessions(state_root)
    assert reg.select_session(rows, "astra-life", kind="codex")["owner_pid"] == ME


def test_name_does_not_leak_to_other_threads_or_other_codex_home(codex_home, state_root, tmp_path):
    reg.register({"session_id": T1, "cwd": "/w"}, codex_home, state_root, owner_pid=ME, name="astra-life")
    other = reg.register({"session_id": T2, "cwd": "/w"}, codex_home, state_root, owner_pid=ME)
    assert other["name"] == "w-22222222"
    other_home = tmp_path / "other-codex-home"
    rec = reg.register(hook_payload(T1, cwd="/elsewhere"), other_home, state_root, owner_pid=ME)
    assert rec["name"] == "elsewhere-11111111", "a different Codex home is a different install; no name carry-over"


def test_default_name_refresh_does_not_flap_when_cwd_missing(codex_home, state_root):
    first = reg.register(hook_payload(T1, cwd="/work/example-project"), codex_home, state_root, owner_pid=ME)
    assert first["name"] == "example-project-11111111"
    later = reg.register(hook_payload(T1, event="PostToolUse", cwd=""), codex_home, state_root, owner_pid=ME)
    assert later["name"] == "example-project-11111111", "a refresh without cwd keeps the established default name"


# --------------------------------------------------------------------------
# writer-lock discovery (ephemeral rows, read-only)
# --------------------------------------------------------------------------

import fcntl  # noqa: E402

linux_only = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="/proc/locks semantics")


def spawn_codex_locker(lock_path, comm="codex"):
    code = (
        "import fcntl,sys,time\n"
        f"open('/proc/self/comm','w').write({comm!r})\n"
        f"fh=open({str(lock_path)!r},'a'); fcntl.flock(fh, fcntl.LOCK_EX)\n"
        "sys.stdout.write('locked\\n'); sys.stdout.flush(); time.sleep(30)\n"
    )
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True, cwd=str(Path.home()))
    assert child.stdout.readline().strip() == "locked"
    return child


@pytest.fixture
def lock_home(tmp_path):
    home = tmp_path / "lock-codex-home"
    (home / "thread-writer-locks").mkdir(parents=True)
    return home


@linux_only
def test_discover_locked_threads_real_child(lock_home, state_root):
    lock = lock_home / "thread-writer-locks" / f"{T1}.lock"
    child = spawn_codex_locker(lock)
    try:
        rows = reg.discover_locked_threads(lock_home, state_root)
        assert len(rows) == 1
        row = rows[0]
        assert row["thread"] == T1 and row["id"] == T1 and row["kind"] == "codex"
        assert row["owner_pid"] == child.pid and row["pid"] == child.pid
        assert row["owner_identity"] == pp.process_identity(child.pid)
        assert row["exe"] == str(Path(sys.executable).resolve())
        assert row["cwd"] == str(Path.home())
        assert row["name"] == f"{Path.home().name}-11111111"
        assert row["source"] == "writer-lock"
        assert row["phase"] == "unknown", "absent lifecycle metadata never means idle"
        assert row["permission_class"] == "unknown" and row["model"] is None
        assert row["initialization_required"] is True, "no stored thread row yet"
        assert row["codex_home"] == str(lock_home)
        assert row["registered_at"] is None and row["updated_at"] is None and row["bridge"] is None
        assert row["transcript_path"] is None and row["permission_mode"] is None
        assert not (state_root / "registry").exists(), "discovery never writes registry files"
    finally:
        child.kill(); child.wait()
    assert reg.discover_locked_threads(lock_home, state_root) == [], "dead holder => stale lock file excluded"
    assert lock.exists(), "discovery never removes lock files"


@linux_only
def test_discover_uses_state_db_metadata_when_row_exists(lock_home, state_root):
    make_state_db(lock_home, [(T1, "/work/db-cwd", DISABLED, "never", 0, "gpt-6-astra"),
                              (T5, "/work/child", DISABLED, "never", 0, "x")],
                  edges=[(T1, T5, "open")])
    locks = lock_home / "thread-writer-locks"
    a = spawn_codex_locker(locks / f"{T1}.lock")
    b = spawn_codex_locker(locks / f"{T5}.lock")
    try:
        rows = {r["thread"]: r for r in reg.discover_locked_threads(lock_home, state_root)}
        assert set(rows) == {T1}, "a subagent child thread lock is never a session"
        row = rows[T1]
        assert row["initialization_required"] is False
        assert row["permission_class"] == "bypass" and row["model"] == "gpt-6-astra"
        assert row["phase"] == "unknown", "DB metadata still never implies idle"
        assert row["cwd"] == str(Path.home()), "live process cwd beats the stored cwd"
    finally:
        for c in (a, b):
            c.kill(); c.wait()


@linux_only
def test_discover_excludes_wrong_name_shared_symlink_and_bad_filenames(lock_home, state_root):
    locks = lock_home / "thread-writer-locks"
    python_holder = spawn_codex_locker(locks / f"{T1}.lock", comm="python3")
    real = locks / f"{T2}.lock"
    real.touch()
    (locks / f"{T3}.lock").symlink_to(real)
    codex_on_real = spawn_codex_locker(real)
    (locks / "not-a-uuid.lock").touch()
    (locks / f"{T4}.lock").touch()  # present but unlocked
    shared = subprocess.Popen([sys.executable, "-c",
        f"import fcntl,sys,time; open('/proc/self/comm','w').write('codex'); fh=open({str(locks / f'{T6}.lock')!r},'a'); fcntl.flock(fh, fcntl.LOCK_SH); sys.stdout.write('l\\n'); sys.stdout.flush(); time.sleep(30)"],
        stdout=subprocess.PIPE, text=True)
    try:
        assert shared.stdout.readline().strip() == "l"
        rows = reg.discover_locked_threads(lock_home, state_root)
        assert [r["thread"] for r in rows] == [T2], "only the exclusive codex-named holder of a real lock file counts"
    finally:
        for c in (python_holder, codex_on_real, shared):
            c.kill(); c.wait()


def test_discover_missing_lock_dir_and_default_home(tmp_path, monkeypatch):
    assert reg.discover_locked_threads(tmp_path / "nowhere") == []
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "env-home"))
    assert reg.default_codex_home() == tmp_path / "env-home"
    assert reg.discover_locked_threads() == []
    monkeypatch.delenv("CODEX_HOME")
    assert reg.default_codex_home() == Path.home() / ".codex"


@linux_only
def test_discovered_row_reuses_stored_name_for_same_thread_and_home(lock_home, state_root):
    make_state_db(lock_home, [(T1, "/w", DISABLED, "never", 0, "m")])
    sleeper = spawn_sleeper()
    try:
        reg.register({"session_id": T1, "cwd": "/w"}, lock_home, state_root, owner_pid=sleeper.pid, name="astra-life")
    finally:
        sleeper.kill(); sleeper.wait()
    child = spawn_codex_locker(lock_home / "thread-writer-locks" / f"{T1}.lock")
    try:
        rows = reg.discover_locked_threads(lock_home, state_root)
        assert rows[0]["name"] == "astra-life"
        assert rows[0]["owner_pid"] == child.pid
        other_home = lock_home.parent / "other-home"
        (other_home / "thread-writer-locks").mkdir(parents=True)
        # same thread id under a different home would not reuse the label
        assert reg._stored_name(state_root, T1, other_home) is None
    finally:
        child.kill(); child.wait()


@linux_only
def test_sessions_merges_lock_rows_with_precedence(lock_home, state_root, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(lock_home))
    make_state_db(lock_home, [(T1, "/w", DISABLED, "never", 0, "m"), (T2, "/w", DISABLED, "never", 0, "m")])
    locks = lock_home / "thread-writer-locks"
    a = spawn_codex_locker(locks / f"{T1}.lock")
    b = spawn_codex_locker(locks / f"{T2}.lock")
    c = spawn_codex_locker(locks / f"{T3}.lock")
    try:
        # T1 has a live hook registration (different pid than the lock holder: registration wins)
        reg.register(hook_payload(T1), lock_home, state_root, owner_pid=ME, name="registered-one")
        # T2 has a live bridge config only
        make_bridge(state_root, T2, owner_pid=b.pid)
        rows = {r["thread"]: r for r in reg.sessions(state_root)}
        assert set(rows) == {T1, T2, T3}
        assert rows[T1]["source"] == "hook:SessionStart" and rows[T1]["owner_pid"] == ME and rows[T1]["initialization_required"] is False
        # T2's bridge owner is the lock holder: the row is enriched from the lock, bridge kept
        assert rows[T2]["source"] == "writer-lock" and rows[T2]["initialization_required"] is False
        assert rows[T2]["bridge"] and rows[T2]["owner_pid"] == b.pid and rows[T2]["cwd"] == str(Path.home())
        assert rows[T3]["source"] == "writer-lock" and rows[T3]["owner_pid"] == c.pid
        assert rows[T3]["phase"] == "unknown" and rows[T3]["initialization_required"] is True
        assert reg.select_session(list(rows.values()), T3[:8], kind="codex")["pid"] == c.pid
        # discovery can be switched off
        off = {r["thread"] for r in reg.sessions(state_root, discover=False)}
        assert off == {T1, T2}
        # sessions() never wrote anything for T3
        assert not (state_root / "registry" / f"{T3}.json").exists()
    finally:
        for p in (a, b, c):
            p.kill(); p.wait()


@linux_only
def test_sessions_scans_homes_from_registrations_bridges_and_argument(tmp_path, state_root, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "default-home"))
    (tmp_path / "default-home" / "thread-writer-locks").mkdir(parents=True)
    reg_home = tmp_path / "reg-home"; (reg_home / "thread-writer-locks").mkdir(parents=True)
    arg_home = tmp_path / "arg-home"; (arg_home / "thread-writer-locks").mkdir(parents=True)
    a = spawn_codex_locker(reg_home / "thread-writer-locks" / f"{T2}.lock")
    b = spawn_codex_locker(arg_home / "thread-writer-locks" / f"{T3}.lock")
    d = spawn_codex_locker(tmp_path / "default-home" / "thread-writer-locks" / f"{T4}.lock")
    try:
        reg.register({"session_id": T1, "cwd": "/w"}, reg_home, state_root, owner_pid=ME)
        rows = {r["thread"]: r["source"] for r in reg.sessions(state_root, codex_homes=[arg_home])}
        assert rows == {T1: "explicit", T2: "writer-lock", T3: "writer-lock", T4: "writer-lock"}
        without_arg = {r["thread"] for r in reg.sessions(state_root)}
        assert without_arg == {T1, T2, T4}
    finally:
        for p in (a, b, d):
            p.kill(); p.wait()


@linux_only
def test_connected_blank_session_keeps_discovery_name_and_initialization(lock_home, state_root, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(lock_home))
    child = spawn_codex_locker(lock_home / "thread-writer-locks" / f"{T1}.lock")
    try:
        before = reg.select_session(list(reg.sessions(state_root)), T1[:8], kind="codex")
        assert before["source"] == "writer-lock" and before["initialization_required"] is True
        expected_name = before["name"]
        assert expected_name.startswith(Path.home().name)
        # connect creates a bridge for the discovered blank session (same owner process)
        make_bridge(state_root, T1, owner_pid=child.pid, runtime_pid=os.getpid())
        after = reg.select_session(list(reg.sessions(state_root)), T1[:8], kind="codex")
        assert after["name"] == expected_name, "bridge attachment must not rename the session to a bare UUID prefix"
        assert after["initialization_required"] is True, "still no stored thread row"
        assert after["phase"] == "unknown"
        assert after["bridge"] and after["bridge"]["runtime_alive"] is True
        assert after["owner_pid"] == child.pid and after["owner_identity"] == pp.process_identity(child.pid)
        assert after["cwd"] == str(Path.home()) and after["exe"] == str(Path(sys.executable).resolve())
        assert reg.select_session(list(reg.sessions(state_root)), expected_name, kind="codex")["thread"] == T1
    finally:
        child.kill(); child.wait()


@linux_only
def test_bridge_owner_not_holding_lock_keeps_plain_bridge_row(lock_home, state_root, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(lock_home))
    child = spawn_codex_locker(lock_home / "thread-writer-locks" / f"{T1}.lock")
    try:
        make_bridge(state_root, T1, owner_pid=ME)  # bridge owner alive but is NOT the lock holder
        row = reg.select_session(list(reg.sessions(state_root)), T1[:8], kind="codex")
        assert row["source"] == "bridge-config" and row["owner_pid"] == ME
        assert row["initialization_required"] is False and row["phase"] == "unknown"
    finally:
        child.kill(); child.wait()


@linux_only
def test_stale_registration_in_custom_home_still_scans_that_home(tmp_path, state_root, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "default-home"))
    (tmp_path / "default-home").mkdir()
    custom = tmp_path / "custom-home"
    (custom / "thread-writer-locks").mkdir(parents=True)
    dead = subprocess.Popen([sys.executable, "-c", "pass"]); dead.wait()
    sleeper = spawn_sleeper()
    try:
        reg.register({"session_id": T1, "cwd": "/w"}, custom, state_root, owner_pid=sleeper.pid, name="custom-astra")
    finally:
        sleeper.kill(); sleeper.wait()
    assert [r["thread"] for r in reg.sessions(state_root, discover=False)] == [], "registration owner is gone"
    child = spawn_codex_locker(custom / "thread-writer-locks" / f"{T1}.lock")
    try:
        rows = {r["thread"]: r for r in reg.sessions(state_root)}
        assert T1 in rows, "the resumed session in the custom home must be discovered via its lock"
        assert rows[T1]["source"] == "writer-lock" and rows[T1]["owner_pid"] == child.pid
        assert rows[T1]["name"] == "custom-astra", "stored name follows the thread"
        assert rows[T1]["codex_home"] == str(custom)
    finally:
        child.kill(); child.wait()


def test_registry_tests_are_isolated_from_real_codex_home():
    assert os.environ.get("CODEX_HOME", "").startswith(("/tmp", str(Path.home() / ".pytest")) ) or "empty-codex-home" in os.environ.get("CODEX_HOME", "")
    assert reg.sessions(Path(os.environ["CODEX_HOME"]) / "no-state") == []
