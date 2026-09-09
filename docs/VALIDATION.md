# Validation

Run from a checkout with Python 3.10 or newer:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q
```

The tests use synthetic processes, sockets, thread stores and temporary homes.
They do not need logged-in agent accounts and do not start model turns.

Coverage includes framing, sender identity, concurrent peers, pinned replies,
delivery and wake allowances, hook routing, acknowledgement timing, read-only
receipts, explicit setup consent, stale registrations, process restart recovery,
installer behavior, and unknown or corrupted state. Linux process fixtures test
real Unix sockets and writer locks; they skip on other platforms where required.

Real Codex/Claude sessions have separately exercised discovery before the first
prompt, Claude-initiated connections, delivery at tool boundaries, idle wake,
Codex-to-Codex replies, and listener recovery. A fresh-home package installation
has exercised setup, doctor and hook entrypoints from an unrelated directory.
Private session IDs, inboxes and operational study logs are not distributed.

Tested native Codex CLI: 0.153.4. Native integrations depend on capabilities and
version-sensitive configuration in Codex and Claude Code. A green unit suite
does not establish compatibility with every CLI build or account.

Linux APIs have been exercised under WSL2. Run both agents in the same WSL
instance. macOS has adapter and simulated tests but needs real-machine
acceptance. Native Windows is unsupported. An untouched Codex tab still needs
its first turn; a process opened before setup needs a quit/resume to load hooks.

Socket writes, hook offers, explicit acknowledgements and completed tasks are
different events. Status reports them separately and leaves missing evidence
unknown. Continuous delivery is the default for new connections; optional finite
message/wake windows pause when exhausted. Burst tests exercise more than 1,000
real socket arrivals, deduplication and receipts after acknowledged history,
plus more than 50 hook deliveries and idle wake cycles without owner renewal.
