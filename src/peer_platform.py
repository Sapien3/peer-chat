"""Portable process/socket primitives for the peer-chat bridge.

Scope: Linux (including WSL) is the tested platform. macOS (POSIX) is
implemented from documented interfaces but is UNTESTED here; every macOS
branch is labelled. Native Windows is unsupported: use WSL.

Design rules:
- stdlib only; ``psutil`` is used opportunistically on macOS when importable.
- Linux ``process_identity`` keeps the exact historical algorithm
  (``boot_id:pid:starttime``) so existing daemon configs stay valid.
- ``iter_claude_processes`` reads only the ``CLAUDE_CONFIG_DIR`` entry from a
  process environment and never materialises the whole environment.
- No function here launches anything, changes permissions, or reads secrets.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import os
import platform as _platform
import socket
import struct
import subprocess
import sys
from pathlib import Path
from typing import Iterator, Optional

try:  # optional, macOS convenience only
    import psutil  # type: ignore
except Exception:  # pragma: no cover - absent on this box
    psutil = None

SOL_LOCAL = 0          # macOS: <sys/un.h>
LOCAL_PEERPID = 2      # macOS: <sys/un.h>
LOCAL_PEERCRED = 1     # macOS: <sys/un.h>, struct xucred

_DEAD_STATES = ("Z", "X")


# --------------------------------------------------------------------------
# platform detection
# --------------------------------------------------------------------------


def _os_name() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "macos"
    return "unsupported"


def is_wsl() -> bool:
    if not sys.platform.startswith("linux"):
        return False
    try:
        release = Path("/proc/sys/kernel/osrelease").read_text().lower()
    except OSError:
        release = _platform.release().lower()
    return "microsoft" in release or "wsl" in release


def supported_platform() -> dict:
    name = _os_name()
    if name == "linux":
        return {"os": "linux", "supported": True, "wsl": is_wsl(), "reason": "tested platform"}
    if name == "macos":
        return {"os": "macos", "supported": True, "wsl": False,
                "reason": "POSIX implementation from documented interfaces; not exercised by the maintainers"}
    hint = "native Windows is unsupported; run the bridge and both agents inside WSL"
    if not sys.platform.startswith(("win", "cygwin", "msys")):
        hint = f"unsupported platform {sys.platform!r}"
    return {"os": "unsupported", "supported": False, "wsl": False, "reason": hint}


def require_supported() -> None:
    info = supported_platform()
    if not info["supported"]:
        raise ValueError(info["reason"])


def socket_path_limit() -> int:
    """Maximum ``sun_path`` length including the NUL terminator."""
    return 104 if _os_name() == "macos" else 108


# --------------------------------------------------------------------------
# process inspection
# --------------------------------------------------------------------------


def _valid_pid(pid) -> bool:
    return isinstance(pid, int) and not isinstance(pid, bool) and pid > 0


def _linux_stat_fields(pid: int) -> Optional[list]:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    end = raw.rfind(")")
    if end < 0:
        return None
    return raw[end + 2:].split()


def _linux_boot_id() -> str:
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def _macos_boot_marker() -> Optional[str]:  # UNTESTED: macOS
    try:
        out = subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    # "{ sec = 1700000000, usec = 123 } Tue Nov 14 ..." -> "1700000000"
    text = out.stdout
    marker = text.split("sec =", 1)[1].split(",", 1)[0].strip() if "sec =" in text else text.strip()
    return marker or None


def _macos_ps(pid: int, fmt: str) -> Optional[str]:  # UNTESTED: macOS
    try:
        out = subprocess.run(["ps", "-o", fmt, "-p", str(pid)], capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    value = out.stdout.strip()
    return value or None


def process_identity(pid) -> Optional[str]:
    """Stable identity for a live process, or None when it is gone or dead.

    Linux: ``boot_id:pid:starttime`` (unchanged historical format).
    macOS (UNTESTED): ``kern.boottime:pid:create_time``.
    """
    if not _valid_pid(pid):
        return None
    name = _os_name()
    if name == "linux":
        fields = _linux_stat_fields(pid)
        if not fields or len(fields) < 20:
            return None
        if fields[0] in _DEAD_STATES:
            return None
        try:
            return _linux_boot_id() + ":" + str(pid) + ":" + fields[19]
        except OSError:
            return None
    if name == "macos":  # UNTESTED: macOS
        boot = _macos_boot_marker()
        if boot is None:
            return None
        state = _macos_ps(pid, "stat=")
        if not state or state.startswith("Z"):
            return None
        if psutil is not None:
            try:
                created = psutil.Process(pid).create_time()
            except Exception:
                return None
            return f"{boot}:{pid}:{int(created)}"
        started = _macos_ps(pid, "lstart=")
        if not started:
            return None
        return f"{boot}:{pid}:{started.replace(' ', '_')}"
    return None


def process_comm(pid) -> Optional[str]:
    if not _valid_pid(pid):
        return None
    name = _os_name()
    if name == "linux":
        try:
            return Path(f"/proc/{pid}/comm").read_text().strip()
        except OSError:
            return None
    if name == "macos":  # UNTESTED: macOS
        comm = _macos_ps(pid, "comm=")
        return Path(comm).name if comm else None
    return None


def process_exe(pid) -> Optional[Path]:
    if not _valid_pid(pid):
        return None
    name = _os_name()
    if name == "linux":
        try:
            return Path(os.readlink(f"/proc/{pid}/exe").split(" (deleted)")[0]).resolve()
        except OSError:
            return None
    if name == "macos":  # UNTESTED: macOS
        if psutil is not None:
            try:
                return Path(psutil.Process(pid).exe())
            except Exception:
                return None
        comm = _macos_ps(pid, "comm=")
        return Path(comm) if comm and comm.startswith("/") else None
    return None


def parent_pid(pid) -> Optional[int]:
    if not _valid_pid(pid):
        return None
    name = _os_name()
    if name == "linux":
        fields = _linux_stat_fields(pid)
        if not fields or len(fields) < 2:
            return None
        try:
            return int(fields[1])
        except ValueError:
            return None
    if name == "macos":  # UNTESTED: macOS
        value = _macos_ps(pid, "ppid=")
        try:
            return int(value) if value else None
        except ValueError:
            return None
    return None


def find_owner_pid(start_pid, comm: str = "codex", max_depth: int = 64) -> Optional[int]:
    """Walk parents from ``start_pid`` (inclusive) to the first process named ``comm``."""
    pid = start_pid
    for _ in range(max_depth):
        if not _valid_pid(pid) or pid == 1:
            return None
        if process_comm(pid) == comm:
            return pid
        pid = parent_pid(pid)
    return None


# --------------------------------------------------------------------------
# file locks (who holds an exclusive flock on a path)
# --------------------------------------------------------------------------


def process_cwd(pid) -> Optional[Path]:
    if not _valid_pid(pid):
        return None
    name = _os_name()
    if name == "linux":
        try:
            return Path(os.readlink(f"/proc/{pid}/cwd"))
        except OSError:
            return None
    if name == "macos":  # UNTESTED: macOS
        try:
            out = subprocess.run(["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"], capture_output=True, text=True, timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            return None
        for line in out.stdout.splitlines():
            if line.startswith("n/"):
                return Path(line[1:])
        return None
    return None


def process_uid(pid) -> Optional[int]:
    if not _valid_pid(pid):
        return None
    name = _os_name()
    if name == "linux":
        try:
            return os.stat(f"/proc/{pid}").st_uid
        except OSError:
            return None
    if name == "macos":  # UNTESTED: macOS
        value = _macos_ps(pid, "uid=")
        try:
            return int(value) if value else None
        except ValueError:
            return None
    return None


def _linux_fd_matches(pid: int, st) -> bool:
    """True when the process holds an open descriptor on exactly this inode."""
    try:
        entries = os.listdir(f"/proc/{pid}/fd")
    except OSError:
        return False
    for fd in entries:
        try:
            info = os.stat(f"/proc/{pid}/fd/{fd}")
        except OSError:
            continue
        if info.st_dev == st.st_dev and info.st_ino == st.st_ino:
            return True
    return False


def flock_holders(path) -> list:
    """Processes holding a BSD flock on ``path``: [{pid, access:'READ'|'WRITE', fd_confirmed}].

    Linux: /proc/locks rows of type FLOCK matched by device major:minor and
    inode, then confirmed through /proc/<pid>/fd. macOS (UNTESTED): lsof lock
    column, 'W' = exclusive whole-file write lock, 'R' = whole-file read lock.
    Never touches the lock itself.
    """
    path = Path(path)
    try:
        if path.is_symlink() or not path.is_file():
            return []
        st = path.stat()
    except OSError:
        return []  # deleted or replaced while we looked
    name = _os_name()
    holders = []
    if name == "linux":
        want_major, want_minor = os.major(st.st_dev), os.minor(st.st_dev)
        try:
            lines = Path("/proc/locks").read_text().splitlines()
        except OSError:
            return []
        for line in lines:
            parts = line.split()
            if len(parts) < 6 or parts[1] != "FLOCK":
                continue
            try:
                pid = int(parts[4])
                maj, mnr, ino = parts[5].split(":")
                if int(ino) != st.st_ino or int(maj, 16) != want_major or int(mnr, 16) != want_minor:
                    continue
            except ValueError:
                continue
            holders.append({"pid": pid, "access": parts[3], "fd_confirmed": _linux_fd_matches(pid, st)})
        return holders
    if name == "macos":  # UNTESTED: macOS
        try:
            out = subprocess.run(["lsof", "-F", "pl", "--", str(path)], capture_output=True, text=True, timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            return []
        if out.returncode != 0:
            return []
        pid = None
        for line in out.stdout.splitlines():
            if line.startswith("p"):
                try:
                    pid = int(line[1:])
                except ValueError:
                    pid = None
            elif line.startswith("l") and pid is not None:
                lock = line[1:2]
                if lock == "W":
                    holders.append({"pid": pid, "access": "WRITE", "fd_confirmed": True})
                elif lock == "R":
                    holders.append({"pid": pid, "access": "READ", "fd_confirmed": True})
        return holders
    return []


def locked_paths(paths) -> list:
    """Subset of ``paths`` that currently have at least one flock holder, using ONE
    kernel/lsof probe for the whole batch. A cheap prefilter for directories full
    of stale lock files; callers still verify each survivor with
    ``exclusive_lock_owner``. Symlinks, missing files and unreadable entries drop out.
    """
    candidates = []
    for raw in paths:
        path = Path(raw)
        try:
            if path.is_symlink() or not path.is_file():
                continue
            st = path.stat()
        except OSError:
            continue
        candidates.append((path, st))
    if not candidates:
        return []
    name = _os_name()
    if name == "linux":
        keys = set()
        try:
            for line in Path("/proc/locks").read_text().splitlines():
                parts = line.split()
                if len(parts) < 6 or parts[1] != "FLOCK":
                    continue
                try:
                    maj, mnr, ino = parts[5].split(":")
                    keys.add((int(maj, 16), int(mnr, 16), int(ino)))
                except ValueError:
                    continue
        except OSError:
            return []
        return [path for path, st in candidates
                if (os.major(st.st_dev), os.minor(st.st_dev), st.st_ino) in keys]
    if name == "macos":  # UNTESTED: macOS
        try:
            out = subprocess.run(["lsof", "-F", "pln", "--", *[str(p) for p, _ in candidates]],
                                 capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return []
        if out.returncode != 0 and not out.stdout:
            return []
        locked = set()
        current_lock = None
        for line in out.stdout.splitlines():
            if line.startswith("l"):
                current_lock = line[1:2]
            elif line.startswith("n"):
                if current_lock in ("W", "R"):
                    locked.add(line[1:])
                current_lock = None
        return [path for path, _ in candidates if str(path) in locked]
    return []


def exclusive_lock_owner(path, comm: Optional[str] = None) -> Optional[dict]:
    """The single live same-user process holding an exclusive flock on ``path``, else None.

    Returns {pid, identity, exe, cwd}. Ambiguous (several holders), shared
    locks, foreign users, dead or mismatched-name processes all yield None.
    """
    def single_write_holder():
        holders = flock_holders(path)
        if len(holders) != 1:
            return None
        holder = holders[0]
        if holder["access"] != "WRITE" or not holder["fd_confirmed"]:
            return None
        return holder["pid"]

    try:
        before = Path(path).stat()
    except OSError:
        return None
    pid = single_write_holder()
    if pid is None:
        return None
    # Snapshot identity first; everything below may be slow (macOS shells out).
    identity = process_identity(pid)
    if identity is None or process_uid(pid) != os.getuid():
        return None
    if comm is not None and process_comm(pid) != comm:
        return None
    exe = process_exe(pid)
    cwd = process_cwd(pid)
    if exe is None:
        return None  # process vanished or is unreadable mid-inspection
    # Recheck: same live process, still the sole exclusive holder, same inode.
    if process_identity(pid) != identity or single_write_holder() != pid:
        return None
    try:
        after = Path(path).stat()
    except OSError:
        return None
    if (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
        return None
    return {"pid": pid, "identity": identity, "exe": exe, "cwd": cwd}


# --------------------------------------------------------------------------
# Claude process discovery
# --------------------------------------------------------------------------


def _linux_environ_value(pid: int, key: bytes) -> Optional[str]:
    """Return one environment value without keeping the rest in memory."""
    prefix = key + b"="
    try:
        with open(f"/proc/{pid}/environ", "rb") as fh:
            buf = b""
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                buf += chunk
                while True:
                    cut = buf.find(b"\0")
                    if cut < 0:
                        break
                    entry, buf = buf[:cut], buf[cut + 1:]
                    if entry.startswith(prefix):
                        return entry[len(prefix):].decode(errors="replace")
            if buf.startswith(prefix):
                return buf[len(prefix):].decode(errors="replace")
    except OSError:
        return None
    return None


def default_claude_config_dir() -> Path:
    return Path(os.environ.get("CLAUDE_CONFIG_DIR") or (Path.home() / ".claude"))


def iter_claude_processes(comm: str = "claude") -> Iterator[dict]:
    """Yield ``{"pid", "config_dir", "exe"}`` for live processes named ``comm``.

    Only ``CLAUDE_CONFIG_DIR`` is read from the target environment.
    """
    name = _os_name()
    if name == "linux":
        try:
            entries = [p for p in Path("/proc").iterdir() if p.name.isdigit()]
        except OSError:
            return
        for entry in entries:
            pid = int(entry.name)
            if process_comm(pid) != comm or process_identity(pid) is None:
                continue
            value = _linux_environ_value(pid, b"CLAUDE_CONFIG_DIR")
            yield {"pid": pid, "config_dir": Path(value) if value else Path.home() / ".claude", "exe": process_exe(pid)}
        return
    if name == "macos":  # UNTESTED: macOS
        if psutil is not None:
            for proc in psutil.process_iter(["pid", "name"]):
                try:
                    if proc.info["name"] != comm:
                        continue
                    value = proc.environ().get("CLAUDE_CONFIG_DIR")
                    yield {"pid": proc.pid, "config_dir": Path(value) if value else Path.home() / ".claude",
                           "exe": Path(proc.exe())}
                except Exception:
                    continue
            return
        try:
            out = subprocess.run(["ps", "-axo", "pid=,comm="], capture_output=True, text=True, timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            return
        for line in out.stdout.splitlines():
            parts = line.split(None, 1)
            if len(parts) == 2 and Path(parts[1]).name == comm and parts[0].isdigit():
                yield {"pid": int(parts[0]), "config_dir": Path.home() / ".claude", "exe": process_exe(int(parts[0]))}


# --------------------------------------------------------------------------
# socket peer credentials
# --------------------------------------------------------------------------


def peer_credentials(sock: socket.socket) -> tuple:
    """Return ``(pid, uid)`` of the process on the other end of a connected AF_UNIX socket."""
    name = _os_name()
    if name == "linux":
        pid, uid, _gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        return pid, uid
    if name == "macos":  # UNTESTED: macOS
        pid = struct.unpack("i", sock.getsockopt(SOL_LOCAL, LOCAL_PEERPID, 4))[0]
        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        uid = ctypes.c_uint32()
        gid = ctypes.c_uint32()
        if libc.getpeereid(sock.fileno(), ctypes.byref(uid), ctypes.byref(gid)) != 0:
            raise OSError(ctypes.get_errno(), "getpeereid failed")
        return pid, int(uid.value)
    raise ValueError(supported_platform()["reason"])


def verify_peer(sock: socket.socket, peer: dict) -> None:
    """Raise ValueError unless the connected peer is exactly the invited process."""
    pid, uid = peer_credentials(sock)
    if uid != os.getuid():
        raise ValueError("Peer socket belongs to another user")
    if pid != peer.get("pid"):
        raise ValueError("Connected process is not the invited peer pid")
    if process_identity(pid) != peer.get("identity"):
        raise ValueError("Invited peer process ended or changed; invite its replacement explicitly")


# --------------------------------------------------------------------------
# default locations
# --------------------------------------------------------------------------


def default_socket_dir() -> Path:
    """Short, per-user directory for the bridge sockets (caller creates it 0700).

    Linux: ``$XDG_RUNTIME_DIR/peer-chat`` or ``/run/user/UID/peer-chat``; if that
    runtime dir is absent (containers, some WSL setups) fall back to
    ``/tmp/peer-chat-UID``. macOS (UNTESTED): ``$TMPDIR`` is often over 60 bytes,
    which leaves no room for ``codex-<uuid>.sock`` under the 104-byte limit, so
    always use ``/tmp/peer-chat-UID``.
    """
    name = _os_name()
    if name == "linux":
        runtime = os.environ.get("XDG_RUNTIME_DIR")
        base = Path(runtime) if runtime else Path(f"/run/user/{os.getuid()}")
        if base.is_dir():
            return base / "peer-chat"
        return Path(f"/tmp/peer-chat-{os.getuid()}")
    if name == "macos":  # UNTESTED: macOS
        return Path(f"/tmp/peer-chat-{os.getuid()}")
    raise ValueError(supported_platform()["reason"])


def default_state_root() -> Path:
    base = os.environ.get("XDG_STATE_HOME")
    return (Path(base) if base else Path.home() / ".local" / "state") / "peer-chat"


def validate_socket_path(path) -> Path:
    path = Path(path)
    if len(str(path).encode()) + 1 > socket_path_limit():
        raise ValueError(f"Socket path exceeds the {socket_path_limit() - 1}-byte limit on this platform")
    return path


__all__ = [
    "supported_platform", "require_supported", "is_wsl", "socket_path_limit", "validate_socket_path",
    "process_identity", "process_comm", "process_exe", "parent_pid", "find_owner_pid", "process_cwd", "process_uid",
    "flock_holders", "exclusive_lock_owner", "locked_paths",
    "iter_claude_processes", "default_claude_config_dir",
    "peer_credentials", "verify_peer", "default_socket_dir", "default_state_root",
]
