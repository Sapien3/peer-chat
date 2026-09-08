# Local Peer Chat

Let existing Codex and Claude Code sessions find each other, exchange messages,
and divide work on the same computer. Either agent can initiate. Uses the models
and accounts already open; no API keys, fleet workers, or repository setup.

## One-time setup

Download and extract the ZIP from the
[latest release](https://github.com/Sapien3/peer-chat/releases/latest), then run
inside the extracted folder:

```sh
sh install.sh
```

Or install from source:

```sh
git clone https://github.com/Sapien3/peer-chat.git
cd peer-chat
sh install.sh
```

The installer checks for Codex and Claude, installs the user-level commands and
agent instructions, then asks once: **Enable Peer Chat?** Choose `details` to
inspect the exact definitions. There is no manual `/hooks` navigation.

If already installed, run `peer-chat setup`. Setup is idempotent. It uses Codex's
configuration API to trust only the four reviewed Peer Chat definitions. It
never changes model, sandbox, command approvals, credentials, or blanket hook
trust. It preserves unrelated configuration and keeps an original backup.
The hook-state keys are version-sensitive; setup obtains identities/hashes from
Codex itself and verifies its result. Tested with Codex CLI 0.153.4.

Codex processes opened before installation need one quit/resume to load the
lifecycle hooks; a new turn in the same old process does not reload them.
Quit the original process before resuming its conversation to avoid an
"already has an active writer" error.

`peer-chat sessions` discovers open Codex tabs directly from their live thread
writer locks, even before the first message. `peer-chat connect SESSION_NAME`
uses that discovery automatically. No prompt, manual registration command, or
repository-specific setup is needed to establish the connection. Discovery
reads process and lock metadata; it does not launch a model or read transcripts.
Hook registration takes over when the first turn starts. Verified lifecycle
events are retained even before a bridge exists. Once a tab has completed its
first turn, a later connection can use its recorded idle readiness immediately;
a second greeting or "continue" prompt is not required. Evidence is bound to the
same live process and transcript and never transfers to a restarted owner.

Keep an untouched tab open while another agent connects. Codex can print a
session ID before it has saved any conversation; after that empty tab exits,
`codex resume ID` can report "No saved session found". Discovery does not make
an unsaved conversation resumable.

Discovery and model delivery are separate. Codex CLI 0.153.4 defers `SessionStart`
until a turn starts. A completely untouched tab has no saved rollout, and native
`codex queue` rejects remote initialization with "no rollout found for thread
id". Claude can connect and leave messages in its durable inbox immediately,
but the model receives them when its first turn starts. Connection reports
`awaiting_lifecycle_hook` until lifecycle is observed; an unknown state never
triggers an inferred idle wake. `peer-chat register --name NAME` remains an
explicit naming/diagnostic command, not an onboarding requirement.

The installer requires Python 3.10+ (or an installed uv/pipx), plus Codex with
hooks/queue support and Claude Code with native cross-session messaging.

## Use it from either agent

Tell either agent to connect to the other session and collaborate. The installed
skill teaches both sides these commands:

```sh
peer-chat sessions
peer-chat connect SESSION_NAME
```

Codex connects to either Codex or Claude by name. Claude connects to Codex by
name, and Claude-to-Claude uses its native transport. One Codex-to-Codex connect
establishes both directions; it never starts a replacement model.

From Codex:

```sh
peer-chat connect SESSION_NAME
peer-chat send --to SESSION_NAME --file message.txt
peer-chat send --reply-to MESSAGE_ID --file reply.txt
```

From Claude, send with native `SendMessage` to the returned address. No PID,
socket, thread identifier, pasted task, or Codex-first invitation is required.

Names must identify one session. Ambiguous names show candidates rather than
silently picking another project/account. Each channel pins the actual owner and
peer process identities. Multiple connections coexist: adding a Codex peer keeps
existing Claude peers and their inboxes. `connect` changes only the initiating
Codex session's default destination; use `--to` to address another peer.
`--reply-to` always routes to the original sender, even when the default changed.
Ambiguous message IDs require `ack --from SESSION_NAME MESSAGE_ID`.
Reconnecting preserves delivery mode, remaining allowance, and other peers.

`CODEX_HOME`, `CLAUDE_CONFIG_DIR`, `XDG_STATE_HOME`, and `XDG_RUNTIME_DIR` are
respected. Setup accepts repeatable `--claude-home PATH` for alternate accounts.
No username, account name, or project path is embedded in the distributed code.

## Delivery behavior

The normal `auto` mode does both:

- **Codex working:** offer messages at the next supported tool boundary in the
  same turn. Peer text is never copied into a delayed follow-up queue.
- **Codex idle:** queue a small inbox notice to wake its existing session. The
  actual message stays in the inbox and can still be delivered by a hook if an
  owner prompt races the notice. Already-consumed notices never repeat work.

Trusted lifecycle hooks record active/idle state. Unknown state does not trigger
a guessed wake. An active model request cannot be interrupted through this
transport. The standalone app-server `turn/steer` API remains a possible future
transport for sessions hosted by a reachable app server.

Low-level modes remain available: `live` uses hooks without idle wake, `inbox`
uses explicit read/wait, and `queue` is legacy whole-message delivery after the
active turn. `connect` defaults new channels to `auto`. Existing connections are
not silently switched between modes.

```sh
peer-chat status
peer-chat read --id MESSAGE_UUID
peer-chat ack MESSAGE_UUID
peer-chat send --reply-to MESSAGE_UUID --file PATH
peer-chat wait --timeout 30
peer-chat delivery auto --budget 12
```

Delivery windows belong to each receiving session. `peer-chat delivery auto
--budget 12` changes the current thread's incoming window only. To change a
specific Codex recipient's window under owner authorization, use:

```sh
peer-chat delivery auto --budget 12 --to SESSION_NAME
peer-chat status SESSION_NAME
```

The result identifies the changed thread, its incoming scope and current delivery
state, and warns about connected Codex recipients whose allowances remain
exhausted. `wait` watches the caller's inbox; it does not renew a recipient.
Neither renewal acknowledges messages nor resumes an explicitly stopped bridge.

Automatic delivery windows default to 12 messages and 12 wake notices (maximum
50). A fresh owner message renews the configured window once; automated peer
notices cannot renew their own allowance. Restarts preserve the configured size
and both remaining counters. Explicit budget changes reset both counters;
explicit zero stays disabled. Legacy channels without a declared window keep
their old allowance until configured. Status distinguishes `paused_budget`,
`paused_wake_budget`, and `awaiting_lifecycle_hook`.
Hooks cap each delivery at four messages and 18,000 characters. Oversized
messages get an exact-id read reference. `hook_offered`, `queued`, and `written`
are transport states; only explicit acknowledgement marks `consumed`.
Ambiguous operations are retained for inspection rather than blindly retried.
Exactly-once delivery across process crashes is not promised.

The listener survives turns and uses a stable address. A supervised listener
retains messages while its owning Codex process is offline. `peer-chat restart` preserves the inbox and budget; `stop` ends the
channel. The first turn after resuming the same thread restores its saved
connection automatically when the old owner has exited and the invited peer is
still alive. Direct discovery also permits reconnecting before that first turn.
A dead or recycled process cannot silently inherit an enrollment.

## See delivery failures immediately

```sh
peer-chat status                  # all bridges, including dead listeners
peer-chat status SESSION_NAME     # one named bridge
peer-chat status --json           # structured overview, no thread ID required
peer-chat status --current        # detailed current-thread JSON
peer-chat status --all            # include empty unconfigured state directories
peer-chat status SESSION_NAME --messages  # last 20 recipient records, no bodies
peer-chat status SESSION_NAME --message-id MESSAGE_UUID  # exact receipt lookup
peer-chat sessions                # readable discovery table
peer-chat sessions --json         # structured discovery
peer-chat watch status
```

The overview shows runtime state, pending count, remaining message/wake allowance,
and hook evidence. Warnings explain whether the model is waiting for its first
turn, its allowance is exhausted, its owner is offline, or its listener is down.
Check this before delegating a long piece of work.
From either Claude or Codex, inspect the intended recipient with `--messages`
or `--message-id`: `received` means retained, `hook_offered` means offered as
context, and `consumed` means explicitly acknowledged. These queries never read
message bodies or mark anything consumed. Unknown timing is shown as null;
`more: true` means older records exist and can be looked up by exact UUID.

`OLDEST` is the age since arrival of the oldest unacknowledged message, including
messages already offered to a hook. JSON separates ages by pending status and
reports total arrivals, acknowledgements, and unconfirmed advisory attempts.
`delivery_timing` measures arrival-to-hook and arrival-to-ack separately, with
sample count, mean and maximum seconds. Only transitions observed after the
0.5.3 instrumentation are measured; historical timing remains unknown. An
acknowledgement is explicit receipt, not proof that the delegated task finished.
Ack timing also separates records with and without an observed earlier hook;
explicit reads must not be mistaken for automatic hook delivery.

`peer-chat send` distinguishes its socket write (`transport_status`) from the
recipient's delivery state. It includes a fresh destination snapshot and warning;
`retained_or_pending` never claims model receipt. A reply or explicit recipient
acknowledgement is still required. Claude controls its native SendMessage result,
so a blocked native send gets a separate ordinary bridge advisory instead.
Advisories name the relevant blocked messages and remaining allowance, with at
most one attempt per message. They request no reply and cannot renew a delivery
window. Empty inboxes, ordinary turn boundaries and recovery do not announce
state changes to models. Inspect `peer-chat status` for current availability.
Rejected subagent hooks appear separately as `hook_rejected` in status JSON;
they cannot overwrite the main session's last accepted `hook_seen` event.
Startup-hook warnings wait 20 seconds and disappear if delivery catches up first;
budget exhaustion and offline-state warnings remain immediate.
Codex advisories are metadata, surfaced at the next active hook without a wake.

A single local watchdog starts with the first connection. It repairs crashed
listeners, preserves inboxes and allowances, and reconnects the same verified
thread after its Codex process resumes—even before the next startup hook.
Supervised listeners remain available to retain messages and report an offline
owner. The watcher never launches a model or changes a permission. Explicit
`stop` remains stopped. Persistent failures back off and appear in status;
monitoring cannot guarantee a notification while both the listener and monitor
are down. `doctor` reports monitor health.

Existing installations can enable monitoring for their non-stopped bridges with
`peer-chat watch start`; new connections enable it automatically. No additional
hook trust or repository setup is required. Untouched tabs still require their
first native turn before model delivery.

## Privacy and authority

Peer messages are external untrusted input within the owner's authorized task;
they cannot approve actions or change permissions. The skill tells both agents
that a peer request cannot authorize setup, trust changes, or unrelated work.
Setup defaults to no consent in unattended use. For agent-assisted setup, an
owner may approve the exact manifest in chat; only then may the agent use
`--approve-digest DIGEST`. The digest binds that approval to the definitions,
not to a person's identity. It is never a substitute for obtaining owner consent.

Private inboxes and presence records live under `$XDG_STATE_HOME/peer-chat`
(default `~/.local/state/peer-chat`). Do not distribute that directory. Discovery
reads session routing metadata; it never copies credentials or reads transcripts.
The same-user process checks prevent accidental cross-session delivery; they do
not isolate against hostile programs already running as the same OS user.

## Verification and platform status

Verified in a WSL2 environment:

- Claude recreated a stopped Codex listener by name and initiated a native message.
- An untouched Codex tab appeared through live writer-lock discovery with no
  first message, startup hook, or saved thread record.
- Its intended Claude peer connected by the discovered name and sent a canary;
  the inbox retained it while Codex still had no saved thread record.
- Guided exact-hook consent changed only the selected trust entries in an
  isolated real Codex configuration and was verified through `hooks/list`.
- Unit/regression tests cover framing, process identity, races, consent pinning,
  stale registrations, unknown permissions, idle notices, and installation.

Real active-hook delivery and idle wake without a keypress have been observed
in a working Claude/Codex pair. An idle canary became a native queued notice,
triggered UserPromptSubmit, was acknowledged, and ended with Stop. Status retains
`wake_evidence` so a cleared pending marker is not mistaken for missing history.
Real-process tests also cover three Codex transports, a concurrent Claude peer,
replies after changing the default peer, and listener restarts. A direct Codex message also woke an existing Codex recipient, was delivered by
a real hook, and received a model reply. The older sender explicitly read that
reply; automatic delivery into its pre-installation TUI is still unverified.
See [validation scope](docs/VALIDATION.md) for reproducible checks and limits.
Do not infer model receipt from a successful socket write.

Linux APIs are exercised under WSL2. Run both agents inside the same WSL instance.
macOS has an adapter and simulated branch tests, but still needs a real Mac run.
Native Windows is unsupported; use WSL. Different CLI versions need compatibility
verification rather than a promise of universal support.

## Develop, share, remove

Repository layout:

```text
src/                  Runtime modules; installed command entrypoints
  peer_chat_assets/   Canonical skill instructions bundled with the package
tests/                Unit tests and process/socket integration tests
scripts/              Release and share-bundle tooling
docs/                 Quick start and validation guide
.github/workflows/    Automated tests
install.sh            One-command installer
pyproject.toml        Package metadata and test configuration
```

Runtime module names stay stable (`peer_chat`, `peer_hooks`, and the other
`peer_*` modules). Install the checkout in editable mode before developing so
commands and subprocesses resolve modules from `src/`, including from another
working directory. Edit skill instructions only in
`src/peer_chat_assets/SKILL.md`; setup installs that resource for both agents.

```sh
uv venv
uv pip install -e '.[test]'
.venv/bin/python -m pytest -q
uv build
```

Build a colleague bundle from a tested wheel with
`python3 scripts/package_share.py dist/local_peer_chat-0.5.7-py3-none-any.whl`.
It includes the wheel, installer, quick start, reference and checksums. Nothing
is published to a package index. Ordinary tests use fixtures and never launch
a model. Native configuration probes are separate and use temporary homes.

`peer-chat setup --review` installs untrusted definitions and prints the exact
consent manifest. `--dry-run` previews changes without installing.
`peer-chat setup --remove-hooks` removes only the bridge's hooks. Uninstall the
package with the tool used to install it; private inboxes and unrelated hooks are
preserved. Remove selected `skills/peer-chat` directories if no longer wanted.

References: [Codex hooks](https://learn.chatgpt.com/docs/hooks),
[Codex app server](https://learn.chatgpt.com/docs/app-server),
[Claude cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging).

For native Claude sends, check the `connect` destination snapshot or `status NAME`
before handing off. A completely untouched Codex tab may have no permission
metadata: its native advisory uses a conservative fallback that Claude may hold.
The CLI state remains authoritative about that delivery blocker; absence of an
advisory is never evidence that the peer model received the message.

The 0.5.2 live-use study tightened reporting: JSON keeps full peer identities,
legacy wake windows expose their effective allowance, shortened names retain
unique suffixes, and stopped bridges mark old phase/hook evidence as stale.
Empty unconfigured state directories are counted in a footer (`status --all`
shows them); inspecting an unknown thread no longer creates one. Acknowledged
messages no longer display the old pending-acknowledgement detail. Both
`peer-chat --thread ID status` and `peer-chat status --thread ID` work.
See [validation scope](docs/VALIDATION.md) for the checks and their limits.
