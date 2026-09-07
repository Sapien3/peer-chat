"""Tests for peer_platform.py. Linux is the tested platform; macOS branches are
exercised only through monkeypatched dispatch so their shape is checked without
claiming they work on real macOS."""

import importlib.util
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "peer_platform.py"
SPEC = importlib.util.spec_from_file_location("peer_platform", MODULE_PATH)
assert SPEC and SPEC.loader
pp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pp)

LINUX = sys.platform.startswith("linux")
linux_only = pytest.mark.skipif(not LINUX, reason="Linux /proc semantics")


def spawn_sleeper(comm=None):
    code = "import sys,time\n"
    if comm is not None:
        code += f"open('/proc/self/comm','w').write({comm!r})\n"
    code += "sys.stdout.write('ready\\n'); sys.stdout.flush(); time.sleep(30)\n"
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "ready"
    return child


# --------------------------------------------------------------------------
# platform detection
# --------------------------------------------------------------------------


def test_supported_platform_shape():
    info = pp.supported_platform()
    assert set(info) == {"os", "supported", "wsl", "reason"}
    assert info["os"] in ("linux", "macos", "unsupported")
    assert isinstance(info["supported"], bool) and isinstance(info["wsl"], bool)
    if LINUX:
        assert info["supported"] and info["os"] == "linux"
        pp.require_supported()


def test_windows_is_unsupported_with_wsl_hint(monkeypatch):
    monkeypatch.setattr(pp.sys, "platform", "win32")
    info = pp.supported_platform()
    assert info == {"os": "unsupported", "supported": False, "wsl": False, "reason": info["reason"]}
    assert "WSL" in info["reason"]
    with pytest.raises(ValueError):
        pp.require_supported()
    with pytest.raises(ValueError):
        pp.default_socket_dir()


def test_macos_is_marked_untested(monkeypatch):
    monkeypatch.setattr(pp.sys, "platform", "darwin")
    info = pp.supported_platform()
    assert info["os"] == "macos" and info["supported"] and not info["wsl"]
    assert "not exercised" in info["reason"]
    assert pp.socket_path_limit() == 104


def test_socket_path_limit_linux(monkeypatch):
    monkeypatch.setattr(pp.sys, "platform", "linux")
    assert pp.socket_path_limit() == 108
    pp.validate_socket_path("/tmp/" + "x" * 100)
    with pytest.raises(ValueError):
        pp.validate_socket_path("/tmp/" + "x" * 103)


@linux_only
def test_is_wsl_matches_kernel_release():
    release = Path("/proc/sys/kernel/osrelease").read_text().lower()
    assert pp.is_wsl() == ("microsoft" in release or "wsl" in release)


# --------------------------------------------------------------------------
# process identity (Linux algorithm must stay byte-compatible)
# --------------------------------------------------------------------------


@linux_only
def test_process_identity_matches_historical_format():
    ident = pp.process_identity(os.getpid())
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    raw = Path(f"/proc/{os.getpid()}/stat").read_text()
    start = raw[raw.rfind(")") + 2:].split()[19]
    assert ident == f"{boot}:{os.getpid()}:{start}"
    assert ident == pp.process_identity(os.getpid())


@pytest.mark.parametrize("bad", [0, -1, None, "12", 1.5, True, 2**22 - 1])
def test_process_identity_invalid_or_missing_is_none(bad):
    assert pp.process_identity(bad) is None
    assert pp.process_comm(bad) is None
    assert pp.process_exe(bad) is None
    assert pp.parent_pid(bad) is None


@linux_only
def test_process_identity_reaped_and_zombie_are_none():
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    for _ in range(300):
        raw = Path(f"/proc/{child.pid}/stat").read_text()
        if raw[raw.rfind(")") + 2:].split()[0] == "Z":
            break
        time.sleep(0.01)
    else:
        child.wait()
        pytest.skip("could not observe zombie")
    try:
        assert pp.process_identity(child.pid) is None
    finally:
        child.wait()
    assert pp.process_identity(child.pid) is None


@linux_only
def test_process_identity_distinct_and_odd_comm():
    child = spawn_sleeper("a) b (c) d")
    try:
        assert pp.process_comm(child.pid) == "a) b (c) d"
        ident = pp.process_identity(child.pid)
        assert ident is not None and ident != pp.process_identity(os.getpid())
        assert ident.split(":")[1] == str(child.pid)
        assert pp.parent_pid(child.pid) == os.getpid()
        assert pp.process_exe(child.pid) == Path(sys.executable).resolve()
    finally:
        child.kill()
        child.wait()


@linux_only
def test_find_owner_pid_walks_parents():
    child = spawn_sleeper("codex")
    try:
        grandchild = subprocess.Popen(
            [sys.executable, "-c", f"import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); print(p.pid, flush=True); time.sleep(30)"],
            stdout=subprocess.PIPE, text=True)
        try:
            inner = int(grandchild.stdout.readline())
            # inner -> grandchild -> this test -> ... The walk must reach whatever
            # "codex" ancestor this test itself has (a real one when launched from
            # Codex, none otherwise), never a sibling and never the fake child.
            assert pp.find_owner_pid(inner) == pp.find_owner_pid(os.getpid())
            assert pp.find_owner_pid(inner) != child.pid
            assert pp.parent_pid(inner) == grandchild.pid
            assert pp.find_owner_pid(inner, comm=pp.process_comm(inner)) == inner, "inclusive of start pid"
            assert pp.find_owner_pid(inner, comm=pp.process_comm(grandchild.pid)) in (inner, grandchild.pid)
            assert pp.find_owner_pid(child.pid) == child.pid
            assert pp.find_owner_pid(child.pid, comm="definitely-not-a-comm") is None
        finally:
            subprocess.run(["kill", str(inner)], check=False)
            grandchild.kill()
            grandchild.wait()
    finally:
        child.kill()
        child.wait()


def test_find_owner_pid_bounded_and_safe():
    assert pp.find_owner_pid(1) is None
    assert pp.find_owner_pid(0) is None
    assert pp.find_owner_pid(None) is None


# --------------------------------------------------------------------------
# Claude process discovery: only CLAUDE_CONFIG_DIR is read
# --------------------------------------------------------------------------


@linux_only
def test_environ_value_reads_single_key_across_chunks(tmp_path, monkeypatch):
    secret = "SECRET-DO-NOT-LEAK"
    child = subprocess.Popen(
        [sys.executable, "-c", "import sys,time; sys.stdout.write('ready\\n'); sys.stdout.flush(); time.sleep(30)"],
        stdout=subprocess.PIPE, text=True,
        env={"PATH": os.environ.get("PATH", ""), "PAD": "p" * 70000, "API_KEY": secret,
             "CLAUDE_CONFIG_DIR": "/some/where/claude", "ZZ": "end"})
    try:
        assert child.stdout.readline().strip() == "ready"
        assert pp._linux_environ_value(child.pid, b"CLAUDE_CONFIG_DIR") == "/some/where/claude"
        assert pp._linux_environ_value(child.pid, b"ZZ") == "end"
        assert pp._linux_environ_value(child.pid, b"MISSING") is None
        assert pp._linux_environ_value(2**22 - 1, b"CLAUDE_CONFIG_DIR") is None
    finally:
        child.kill()
        child.wait()


@linux_only
def test_iter_claude_processes_finds_named_process_and_config_dir():
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import sys,time; open('/proc/self/comm','w').write('claude'); sys.stdout.write('ready\\n'); sys.stdout.flush(); time.sleep(30)"],
        stdout=subprocess.PIPE, text=True, env={**os.environ, "CLAUDE_CONFIG_DIR": "/tmp/alt-claude", "TOKEN": "SECRET-X"})
    try:
        assert child.stdout.readline().strip() == "ready"
        rows = {r["pid"]: r for r in pp.iter_claude_processes()}
        assert child.pid in rows
        row = rows[child.pid]
        assert set(row) == {"pid", "config_dir", "exe"}
        assert row["config_dir"] == Path("/tmp/alt-claude")
        assert row["exe"] == Path(sys.executable).resolve()
        assert "SECRET-X" not in repr(row)
        assert os.getpid() not in rows
    finally:
        child.kill()
        child.wait()


@linux_only
def test_iter_claude_processes_default_config_dir_when_env_absent():
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import sys,time; open('/proc/self/comm','w').write('claude'); sys.stdout.write('ready\\n'); sys.stdout.flush(); time.sleep(30)"],
        stdout=subprocess.PIPE, text=True, env={"PATH": os.environ.get("PATH", "")})
    try:
        assert child.stdout.readline().strip() == "ready"
        rows = {r["pid"]: r for r in pp.iter_claude_processes()}
        assert rows[child.pid]["config_dir"] == Path.home() / ".claude"
    finally:
        child.kill()
        child.wait()


def test_iter_claude_processes_custom_comm_filters():
    assert all(r["pid"] != os.getpid() for r in pp.iter_claude_processes(comm="no-such-comm-name"))


# --------------------------------------------------------------------------
# socket peer credentials
# --------------------------------------------------------------------------


@pytest.fixture
def unix_pair(tmp_path):
    path = tmp_path / "p.sock"
    assert len(str(path).encode()) < 100
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(path))
    srv.listen(1)
    cli = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    cli.connect(str(path))
    conn, _ = srv.accept()
    yield conn, cli
    conn.close()
    cli.close()
    srv.close()


@linux_only
def test_peer_credentials_same_process(unix_pair):
    conn, cli = unix_pair
    assert pp.peer_credentials(conn) == (os.getpid(), os.getuid())
    assert pp.peer_credentials(cli) == (os.getpid(), os.getuid())


@linux_only
def test_verify_peer_accepts_exact_process_and_rejects_others(unix_pair):
    conn, _ = unix_pair
    me = {"pid": os.getpid(), "identity": pp.process_identity(os.getpid())}
    pp.verify_peer(conn, me)
    with pytest.raises(ValueError):
        pp.verify_peer(conn, {"pid": os.getpid(), "identity": "boot:0:0"})
    with pytest.raises(ValueError):
        pp.verify_peer(conn, {"pid": os.getpid() + 1, "identity": me["identity"]})
    with pytest.raises(ValueError):
        pp.verify_peer(conn, {})


@linux_only
def test_verify_peer_rejects_other_process_on_the_wire(tmp_path):
    path = tmp_path / "o.sock"
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(path))
    srv.listen(1)
    other = subprocess.Popen([sys.executable, "-c",
                              f"import socket,time; s=socket.socket(socket.AF_UNIX); s.connect({str(path)!r}); time.sleep(30)"])
    try:
        srv.settimeout(5)
        conn, _ = srv.accept()
        pid, uid = pp.peer_credentials(conn)
        assert pid == other.pid and uid == os.getuid()
        pp.verify_peer(conn, {"pid": other.pid, "identity": pp.process_identity(other.pid)})
        with pytest.raises(ValueError):
            pp.verify_peer(conn, {"pid": os.getpid(), "identity": pp.process_identity(os.getpid())})
        conn.close()
    finally:
        other.kill()
        other.wait()
        srv.close()


def test_verify_peer_rejects_other_uid(monkeypatch, tmp_path):
    monkeypatch.setattr(pp, "peer_credentials", lambda sock: (os.getpid(), os.getuid() + 1))
    with pytest.raises(ValueError, match="another user"):
        pp.verify_peer(object(), {"pid": os.getpid(), "identity": pp.process_identity(os.getpid())})


def test_peer_credentials_unsupported_platform(monkeypatch):
    monkeypatch.setattr(pp.sys, "platform", "win32")
    with pytest.raises(ValueError):
        pp.peer_credentials(object())


# --------------------------------------------------------------------------
# default locations
# --------------------------------------------------------------------------


@linux_only
def test_default_socket_dir_linux(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    assert pp.default_socket_dir() == tmp_path / "peer-chat"
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "absent"))
    assert pp.default_socket_dir() == Path(f"/tmp/peer-chat-{os.getuid()}")
    monkeypatch.delenv("XDG_RUNTIME_DIR")
    expected = Path(f"/run/user/{os.getuid()}")
    got = pp.default_socket_dir()
    assert got == (expected / "peer-chat" if expected.is_dir() else Path(f"/tmp/peer-chat-{os.getuid()}"))
    assert not got.exists() or got.is_dir(), "adapter must not create the directory itself"


def test_default_socket_dir_macos_is_short_and_ignores_tmpdir(monkeypatch):
    monkeypatch.setattr(pp.sys, "platform", "darwin")
    monkeypatch.setenv("TMPDIR", "/private/var/folders/xx/abcdefghijklmnopqrstuvwxyz0123456789/T")
    got = pp.default_socket_dir()
    assert got == Path(f"/tmp/peer-chat-{os.getuid()}")
    sock = got / ("codex-" + "0" * 8 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 12 + ".sock")
    pp.validate_socket_path(sock)  # fits in 104 bytes with room to spare
    assert len(str(sock).encode()) + 1 < 104 - 20


def test_default_state_root(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    assert pp.default_state_root() == tmp_path / "peer-chat"
    monkeypatch.delenv("XDG_STATE_HOME")
    assert pp.default_state_root() == Path.home() / ".local" / "state" / "peer-chat"


def test_default_claude_config_dir(monkeypatch):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", "/x/y")
    assert pp.default_claude_config_dir() == Path("/x/y")
    monkeypatch.delenv("CLAUDE_CONFIG_DIR")
    assert pp.default_claude_config_dir() == Path.home() / ".claude"


# --------------------------------------------------------------------------
# macOS branches: shape only, via monkeypatched helpers (UNTESTED on real macOS)
# --------------------------------------------------------------------------


def test_macos_identity_shape_via_ps(monkeypatch):
    monkeypatch.setattr(pp.sys, "platform", "darwin")
    monkeypatch.setattr(pp, "psutil", None)
    monkeypatch.setattr(pp, "_macos_boot_marker", lambda: "1700000000")
    table = {"stat=": "S+", "lstart=": "Tue Nov 14 10:00:00 2026", "ppid=": "7", "comm=": "/usr/local/bin/claude"}
    monkeypatch.setattr(pp, "_macos_ps", lambda pid, fmt: table.get(fmt))
    assert pp.process_identity(42) == "1700000000:42:Tue_Nov_14_10:00:00_2026"
    assert pp.parent_pid(42) == 7
    assert pp.process_comm(42) == "claude"
    assert pp.process_exe(42) == Path("/usr/local/bin/claude")
    monkeypatch.setattr(pp, "_macos_ps", lambda pid, fmt: "Z" if fmt == "stat=" else table.get(fmt))
    assert pp.process_identity(42) is None
    monkeypatch.setattr(pp, "_macos_ps", lambda pid, fmt: None)
    assert pp.process_identity(42) is None and pp.parent_pid(42) is None


def test_macos_boot_marker_parsing(monkeypatch):
    class R:
        returncode = 0
        stdout = "{ sec = 1700000000, usec = 5 } Tue Nov 14 09:00:00 2026\n"
    monkeypatch.setattr(pp.subprocess, "run", lambda *a, **k: R())
    assert pp._macos_boot_marker() == "1700000000"


def test_module_exports_contract():
    for name in ("supported_platform", "process_identity", "process_exe", "parent_pid", "process_comm",
                 "find_owner_pid", "iter_claude_processes", "peer_credentials", "verify_peer",
                 "default_socket_dir", "default_state_root", "socket_path_limit", "validate_socket_path"):
        assert callable(getattr(pp, name)), name


# --------------------------------------------------------------------------
# file locks: who holds an exclusive flock (Linux real child; macOS shape only)
# --------------------------------------------------------------------------


def spawn_locker(path, mode="LOCK_EX", comm=None):
    code = (
        "import fcntl,sys,time\n"
        + (f"open('/proc/self/comm','w').write({comm!r})\n" if comm else "")
        + f"fh=open({str(path)!r},'a'); fcntl.flock(fh, fcntl.{mode})\n"
        "sys.stdout.write('locked\\n'); sys.stdout.flush(); time.sleep(30)\n"
    )
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "locked"
    return child


@linux_only
def test_flock_holders_real_exclusive_lock(tmp_path):
    lock = tmp_path / "t.lock"
    lock.touch()
    assert pp.flock_holders(lock) == []
    child = spawn_locker(lock)
    try:
        holders = pp.flock_holders(lock)
        assert holders == [{"pid": child.pid, "access": "WRITE", "fd_confirmed": True}]
        owner = pp.exclusive_lock_owner(lock)
        assert owner["pid"] == child.pid
        assert owner["identity"] == pp.process_identity(child.pid)
        assert owner["exe"] == Path(sys.executable).resolve()
        assert owner["cwd"] == Path(os.getcwd())
        assert pp.exclusive_lock_owner(lock, comm=pp.process_comm(child.pid))["pid"] == child.pid
        assert pp.exclusive_lock_owner(lock, comm="codex") is None, "name mismatch is not an owner"
    finally:
        child.kill()
        child.wait()
    assert pp.flock_holders(lock) == []
    assert pp.exclusive_lock_owner(lock) is None


@linux_only
def test_flock_holders_shared_and_multiple_are_not_exclusive(tmp_path):
    lock = tmp_path / "s.lock"
    lock.touch()
    a = spawn_locker(lock, "LOCK_SH")
    b = spawn_locker(lock, "LOCK_SH")
    try:
        holders = pp.flock_holders(lock)
        assert {h["pid"] for h in holders} == {a.pid, b.pid}
        assert all(h["access"] == "READ" for h in holders)
        assert pp.exclusive_lock_owner(lock) is None
        b.kill(); b.wait()
        assert pp.exclusive_lock_owner(lock) is None, "a single shared lock is still not exclusive"
    finally:
        for c in (a, b):
            c.kill(); c.wait()


@linux_only
def test_flock_holders_ignores_symlink_missing_and_open_without_lock(tmp_path):
    real = tmp_path / "real.lock"
    real.touch()
    link = tmp_path / "link.lock"
    link.symlink_to(real)
    child = spawn_locker(real)
    try:
        assert pp.flock_holders(link) == []
        assert pp.exclusive_lock_owner(link) is None
        assert pp.flock_holders(tmp_path / "missing.lock") == []
        opener = subprocess.Popen([sys.executable, "-c", f"import sys,time; fh=open({str(tmp_path / 'other.lock')!r},'a'); sys.stdout.write('o\\n'); sys.stdout.flush(); time.sleep(30)"], stdout=subprocess.PIPE, text=True)
        try:
            assert opener.stdout.readline().strip() == "o"
            assert pp.flock_holders(tmp_path / "other.lock") == [], "an open descriptor without a lock is not a holder"
        finally:
            opener.kill(); opener.wait()
    finally:
        child.kill(); child.wait()


@linux_only
def test_process_cwd_and_uid_linux():
    assert pp.process_cwd(os.getpid()) == Path(os.getcwd())
    assert pp.process_uid(os.getpid()) == os.getuid()
    assert pp.process_cwd(2**22 - 1) is None and pp.process_uid(2**22 - 1) is None
    assert pp.process_cwd(None) is None and pp.process_uid(-1) is None


def test_exclusive_lock_owner_rejects_other_uid(tmp_path, monkeypatch):
    lock = tmp_path / "u.lock"
    lock.touch()
    monkeypatch.setattr(pp, "flock_holders", lambda p: [{"pid": os.getpid(), "access": "WRITE", "fd_confirmed": True}])
    monkeypatch.setattr(pp, "process_uid", lambda pid: os.getuid() + 1)
    assert pp.exclusive_lock_owner(lock) is None
    monkeypatch.setattr(pp, "process_uid", lambda pid: os.getuid())
    assert pp.exclusive_lock_owner(lock)["pid"] == os.getpid()
    monkeypatch.setattr(pp, "flock_holders", lambda p: [{"pid": os.getpid(), "access": "WRITE", "fd_confirmed": False}])
    assert pp.exclusive_lock_owner(lock) is None, "lock row without a confirming descriptor is not trusted"


def test_flock_holders_macos_lsof_shape(tmp_path, monkeypatch):
    lock = tmp_path / "m.lock"
    lock.touch()
    monkeypatch.setattr(pp.sys, "platform", "darwin")

    class R:
        returncode = 0
        stdout = "p4242\nf3\nlW\np4343\nf5\nlR\np4444\nf7\nl \n"
    monkeypatch.setattr(pp.subprocess, "run", lambda *a, **k: R())
    assert pp.flock_holders(lock) == [
        {"pid": 4242, "access": "WRITE", "fd_confirmed": True},
        {"pid": 4343, "access": "READ", "fd_confirmed": True},
    ]


def test_flock_holders_unsupported_platform(tmp_path, monkeypatch):
    lock = tmp_path / "w.lock"
    lock.touch()
    monkeypatch.setattr(pp.sys, "platform", "win32")
    assert pp.flock_holders(lock) == []
    assert pp.exclusive_lock_owner(lock) is None


# --------------------------------------------------------------------------
# lock discovery robustness: races, vanished processes, lsof failure
# --------------------------------------------------------------------------


def test_flock_holders_survives_path_deleted_mid_scan(tmp_path, monkeypatch):
    lock = tmp_path / "gone.lock"
    lock.touch()
    real_stat = pp.Path.stat

    def racing_stat(self, *a, **k):
        if self == lock:
            raise FileNotFoundError(str(self))
        return real_stat(self, *a, **k)
    monkeypatch.setattr(pp.Path, "stat", racing_stat)
    assert pp.flock_holders(lock) == []
    assert pp.exclusive_lock_owner(lock) is None


@linux_only
def test_flock_holders_ignores_malformed_proc_locks_lines(tmp_path, monkeypatch):
    lock = tmp_path / "m.lock"
    lock.touch()
    child = spawn_locker(lock)
    try:
        st = lock.stat()
        good = f"1: FLOCK  ADVISORY  WRITE {child.pid} {os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino} 0 EOF"
        text = "\n".join([
            "2: POSIX  ADVISORY  WRITE 1 00:01:2 0 EOF",
            "3: FLOCK  ADVISORY  WRITE notapid zz:zz:zz 0 EOF",
            "4: FLOCK",
            f"5: FLOCK  ADVISORY  WRITE {child.pid} {os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino + 1} 0 EOF",
            good,
        ])
        real_read = pp.Path.read_text

        def fake_read(self, *a, **k):
            return text if str(self) == "/proc/locks" else real_read(self, *a, **k)
        monkeypatch.setattr(pp.Path, "read_text", fake_read)
        assert pp.flock_holders(lock) == [{"pid": child.pid, "access": "WRITE", "fd_confirmed": True}]
    finally:
        child.kill(); child.wait()


@linux_only
def test_exclusive_lock_owner_rechecks_identity_and_holder(tmp_path, monkeypatch):
    lock = tmp_path / "r.lock"
    lock.touch()
    child = spawn_locker(lock)
    try:
        real_identity = pp.process_identity
        calls = {"n": 0}

        def flapping_identity(pid):
            calls["n"] += 1
            value = real_identity(pid)
            return value if calls["n"] < 2 else (value + ":changed" if value else None)
        monkeypatch.setattr(pp, "process_identity", flapping_identity)
        assert pp.exclusive_lock_owner(lock) is None, "identity changed between snapshot and recheck"
        monkeypatch.setattr(pp, "process_identity", real_identity)
        monkeypatch.setattr(pp, "process_exe", lambda pid: None)
        assert pp.exclusive_lock_owner(lock) is None, "unreadable executable means the process is not trusted"
        monkeypatch.undo()
        real_holders = pp.flock_holders
        seq = {"n": 0}

        def holder_then_gone(path):
            seq["n"] += 1
            return real_holders(path) if seq["n"] == 1 else []
        monkeypatch.setattr(pp, "flock_holders", holder_then_gone)
        assert pp.exclusive_lock_owner(lock) is None, "lock released during inspection"
    finally:
        child.kill(); child.wait()


def test_exclusive_lock_owner_detects_inode_replacement(tmp_path, monkeypatch):
    lock = tmp_path / "i.lock"
    lock.touch()
    monkeypatch.setattr(pp, "flock_holders", lambda p: [{"pid": os.getpid(), "access": "WRITE", "fd_confirmed": True}])
    real_exe = pp.process_exe

    def replace_file_then_exe(pid):
        other = tmp_path / "replacement.lock"
        other.touch()  # coexists with lock, so it is guaranteed a different inode
        os.replace(other, lock)  # same path, new inode
        return real_exe(pid)
    monkeypatch.setattr(pp, "process_exe", replace_file_then_exe)
    assert pp.exclusive_lock_owner(lock) is None


def test_flock_holders_macos_lsof_nonzero_exit_means_no_rows(tmp_path, monkeypatch):
    lock = tmp_path / "e.lock"
    lock.touch()
    monkeypatch.setattr(pp.sys, "platform", "darwin")

    class R:
        returncode = 1
        stdout = "p4242\nf3\nlW\n"
    monkeypatch.setattr(pp.subprocess, "run", lambda *a, **k: R())
    assert pp.flock_holders(lock) == []


@linux_only
def test_locked_paths_batch_prefilter_linux(tmp_path):
    held = tmp_path / "held.lock"; held.touch()
    stale = tmp_path / "stale.lock"; stale.touch()
    link = tmp_path / "link.lock"; link.symlink_to(held)
    missing = tmp_path / "missing.lock"
    child = spawn_locker(held)
    try:
        assert pp.locked_paths([stale, link, missing, held, str(held)]) == [held, Path(str(held))]
        assert pp.locked_paths([stale, missing]) == []
        assert pp.locked_paths([]) == []
    finally:
        child.kill(); child.wait()
    assert pp.locked_paths([held, stale]) == []


def test_locked_paths_macos_batch_shape(tmp_path, monkeypatch):
    a = tmp_path / "a.lock"; a.touch()
    b = tmp_path / "b.lock"; b.touch()
    c = tmp_path / "c.lock"; c.touch()
    monkeypatch.setattr(pp.sys, "platform", "darwin")
    seen = {}

    class R:
        returncode = 0
        stdout = f"p1\nf3\nlW\nn{a}\nf4\nl \nn{c}\np2\nf5\nlR\nn{b}\n"

    def fake_run(cmd, *args, **kw):
        seen["cmd"] = cmd
        return R()
    monkeypatch.setattr(pp.subprocess, "run", fake_run)
    assert pp.locked_paths([a, b, c]) == [a, b]
    assert seen["cmd"][:4] == ["lsof", "-F", "pln", "--"] and len(seen["cmd"]) == 7, "one lsof call for the batch"
