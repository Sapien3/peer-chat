"""Bounded configuration RPCs to a private Codex app-server; no model runs."""
from __future__ import annotations

import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import time

import peer_platform as pp


def codex_binary():
    owner = pp.find_owner_pid(os.getppid())
    executable = pp.process_exe(owner) if owner else None
    return str(executable or shutil.which("codex") or "codex")


class CodexConfig:
    def __init__(self, home, binary=None):
        self.home = Path(home).absolute()
        env = dict(os.environ, CODEX_HOME=str(self.home))
        self.process = subprocess.Popen([binary or codex_binary(), "app-server"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b""
        self.counter = 0
        try:
            self.call("initialize", {"clientInfo": {"name": "peer_chat_setup", "version": "0.3.0"},
                                    "capabilities": {"experimentalApi": True}})
        except BaseException:
            self.close()
            raise

    def call(self, method, params, timeout=10):
        if method not in {"initialize", "hooks/list", "config/read", "config/batchWrite"}:
            raise ValueError("This setup client only supports configuration operations")
        self.counter += 1
        mid = self.counter
        self.process.stdin.write((json.dumps({"id": mid, "method": method, "params": params}) + "\n").encode())
        self.process.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            while b"\n" in self.buffer:
                raw, self.buffer = self.buffer.split(b"\n", 1)
                reply = json.loads(raw)
                if "method" in reply and "id" in reply:
                    raise ValueError("Codex requested an interactive operation during configuration setup")
                if reply.get("id") != mid:
                    continue
                if "error" in reply:
                    raise ValueError(f"Codex rejected {method} (code {reply['error'].get('code')})")
                return reply["result"]
            if self.selector.select(max(0, deadline - time.monotonic())):
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise ValueError("Codex configuration process closed unexpectedly")
                self.buffer += chunk
                if len(self.buffer) > 16 * 1024 * 1024:
                    raise ValueError("Codex configuration response exceeds limit")
        raise TimeoutError(f"Codex {method} timed out")

    def hooks(self):
        result = self.call("hooks/list", {"cwds": [str(self.home)]})
        data = result.get("data", [])
        if len(data) != 1 or data[0].get("errors"):
            raise ValueError("Codex could not load the configured hooks")
        return data[0].get("hooks", [])

    def close(self):
        self.selector.close()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        for stream in (self.process.stdin, self.process.stdout):
            if stream:
                stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
