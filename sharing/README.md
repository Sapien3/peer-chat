# Peer Chat — quick start

Connect your existing Codex and Claude Code sessions on one computer, in either
direction. Codex sessions can also connect to each other. Uses your existing
accounts; no separate API key or project setup.

## Install once

You need Codex and Claude Code installed and signed in, plus Python 3.10+ with
venv support, uv, or pipx. Use both agents under the same operating-system user.

1. Extract this ZIP.
2. Open a terminal in the extracted `peer-chat-0.5.4` folder and run:

   ```sh
   sh install.sh
   ```

3. Answer **yes** to **Enable Peer Chat?** This enables four specific Codex hooks
   and installs instructions for both agents. No manual `/hooks` setup.
4. Open your agent sessions after installation. If a Codex session was already
   open, quit it once and resume it before using the bridge. Quit the original
   process before resuming; two processes cannot write the same conversation.

The installer uses the included wheel and installs commands in your user account.
It may need internet access for Python tooling or dependencies. No administrator
access is needed. If `peer-chat` is not on PATH after installation, reopen your
terminal; the Python fallback installs it in `~/.local/bin`.

## Collaborate

Give each agent its initial task, then tell either one:

> Use peer-chat to find my other session, connect to it, and divide this task
> between you. Coordinate through the bridge.

The agents discover session names themselves. If several names could match,
identify the one you want. You do not need to copy messages between tabs.
A completely untouched Codex tab still needs its first prompt before its model
can receive messages; discovery alone does not initialize a conversation.

## See what is happening

```sh
peer-chat sessions
peer-chat status
```

The agents can inspect individual messages without exposing their bodies:

```sh
peer-chat status SESSION_NAME --messages
peer-chat status SESSION_NAME --message-id MESSAGE_UUID
```

`received` means retained in the inbox, `hook_offered` means offered as context,
and `consumed` means explicitly acknowledged. Acknowledgement does not mean the
task is finished. Message and wake allowances bound automated exchanges; the
default is 12 each. An exhausted allowance pauses delivery and is reported.

## Compatibility

- Linux/WSL: exercised on WSL2. On Windows, run both agents inside the same WSL
  instance; native Windows is not supported.
- macOS: adapter and automated tests exist, but a real Mac remains unverified.
- Tested Codex CLI: 0.153.4. Codex must expose hooks/queue support, and Claude Code
  must expose native cross-session messaging. Different CLI versions or accounts
  without that capability may need compatibility work.
- This connects sessions on the same machine, not sessions on two colleagues'
  separate computers.

The bundle contains code and documentation only, with no sender accounts,
credentials, configuration, session history or inboxes. See `REFERENCE.md` for
alternate account directories and troubleshooting. `SHA256SUMS` lists the
included files' checksums; on Linux run `sha256sum -c SHA256SUMS` to check them.
