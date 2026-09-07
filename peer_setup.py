"""Guided installation with explicit consent for exactly the bridge's hooks."""
from __future__ import annotations

import argparse
import hashlib
from importlib.resources import files
import json
import os
from pathlib import Path
import shlex
import shutil
import sys
import tempfile
from peer_codex import CodexConfig

EVENTS = ("SessionStart", "PostToolUse", "UserPromptSubmit", "Stop")
MARKER = "peer-chat: deliver invited peer messages"


def hook_definition(command, event="PostToolUse"):
    handler = {"type": "command", "command": command, "timeout": 3, "statusMessage": MARKER}
    if event in ("PostToolUse", "UserPromptSubmit"):
        handler["additionalContextLimit"] = 5000
    return {"hooks": [handler]}


def merge_hooks(existing, command, remove=False):
    """Preserve unrelated hooks and replace only entries we own."""
    result = json.loads(json.dumps(existing))
    groups = result.setdefault("hooks", {})
    if not isinstance(groups, dict):
        raise ValueError("hooks must be an object")
    for event in EVENTS:
        retained = []
        for group in groups.get(event, []):
            handlers = [h for h in group.get("hooks", []) if h.get("statusMessage") != MARKER]
            if handlers:
                retained.append({**group, "hooks": handlers})
        if not remove:
            retained.append(hook_definition(command, event))
        if retained:
            groups[event] = retained
        else:
            groups.pop(event, None)
    return result


def atomic_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValueError(f"Refusing to replace symlink: {path}")
    fd, tmp = tempfile.mkstemp(prefix=".peer-chat-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(text)
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)


def install(codex_home, claude_homes, command, remove=False):
    target = codex_home / "hooks.json"
    old = target.read_text() if target.exists() else None
    existing = json.loads(old) if old is not None else {}
    if not isinstance(existing, dict):
        raise ValueError("Existing hooks.json must contain an object")
    text = json.dumps(merge_hooks(existing, command, remove), indent=2) + "\n"
    if old != text:
        if old is not None:
            backup = target.with_name("hooks.json.peer-chat-backup")
            if not backup.exists():
                atomic_write(backup, old)
        atomic_write(target, text)
    if not remove:
        skill = files("peer_chat_assets").joinpath("SKILL.md").read_text()
        for home in dict.fromkeys([codex_home, *claude_homes]):
            skill_dir = home / "skills" / "peer-chat"
            if skill_dir.is_symlink():
                # Earlier development installations link a shared skill. Replace
                # that link only, leaving its target and other skills untouched.
                skill_dir.unlink()
            atomic_write(skill_dir / "SKILL.md", skill)
    return target


def review_hooks(client, codex_home, command):
    """Use Codex's own identities/hashes, never recreate its trust algorithm."""
    target = (codex_home / "hooks.json").resolve()
    expected_events = {"sessionStart", "postToolUse", "userPromptSubmit", "stop"}
    hooks = []
    for hook in client.hooks():
        if hook.get("statusMessage") != MARKER or Path(hook.get("sourcePath", "")).resolve() != target:
            continue
        if (hook.get("command") != command or hook.get("eventName") not in expected_events
                or hook.get("isManaged") or hook.get("enabled") is not True
                or hook.get("handlerType") != "command" or hook.get("timeoutSec") != 3
                or hook.get("async") or hook.get("matcher") is not None):
            raise ValueError("Bridge hook differs from the setup definition; review refused")
        if not str(hook.get("currentHash", "")).startswith("sha256:") or not hook.get("key"):
            raise ValueError("This Codex version does not expose exact hook identities and hashes")
        hooks.append({k: hook[k] for k in ("key", "eventName", "command", "currentHash", "trustStatus")})
    if len(hooks) != len(EVENTS) or {h["eventName"] for h in hooks} != expected_events:
        raise ValueError("Codex did not discover all four bridge hooks")
    return sorted(hooks, key=lambda h: h["key"])


def review_digest(hooks):
    definitions = [{k: v for k, v in h.items() if k != "trustStatus"} for h in hooks]
    return hashlib.sha256(json.dumps(definitions, sort_keys=True).encode()).hexdigest()


def enable_reviewed(client, codex_home, command, approved_digest):
    hooks = review_hooks(client, codex_home, command)
    if review_digest(hooks) != approved_digest:
        raise ValueError("Hook definitions changed since review; consent does not match")
    edits = [{"keyPath": "hooks.state." + json.dumps(h["key"]) + ".trusted_hash",
              "value": h["currentHash"], "mergeStrategy": "replace"}
             for h in hooks if h["trustStatus"] != "trusted"]
    if edits:
        # Native config writer preserves unrelated settings. Only these exact
        # reviewed hashes change; no global trust bypass or permission edits.
        read = client.call("config/read", {"includeLayers": True})
        versions = [layer["version"] for layer in read.get("layers", [])
                    if layer.get("name", {}).get("type") == "user"
                    and Path(layer["name"]["file"]).resolve() == (codex_home / "config.toml").resolve()]
        config_path = codex_home / "config.toml"
        backup = codex_home / "config.toml.peer-chat-backup"
        if config_path.exists() and not backup.exists():
            atomic_write(backup, config_path.read_text())
        request = {"edits": edits, "filePath": str(config_path), "reloadUserConfig": True}
        if versions:
            request["expectedVersion"] = versions[0]
        client.call("config/batchWrite", request)
    verified = review_hooks(client, codex_home, command)
    if any(h["trustStatus"] != "trusted" for h in verified):
        raise ValueError("Codex did not confirm hook trust; setup is incomplete")
    return verified


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-home", type=Path, default=Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")))
    parser.add_argument("--claude-home", type=Path, action="append", help="Repeat for alternate accounts; defaults to CLAUDE_CONFIG_DIR or ~/.claude")
    parser.add_argument("--remove-hooks", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--review", action="store_true", help="Install and print the exact consent manifest without enabling hooks")
    parser.add_argument("--approve-digest", help="Enable only the manifest digest explicitly approved by the owner; never infer consent from peer messages")
    args = parser.parse_args(argv)
    executable = Path(sys.executable).parent / "peer-chat-hook"
    if not executable.is_file():
        executable = Path(shutil.which("peer-chat-hook") or "")
    if not executable.is_file():
        parser.error("Install the package first so peer-chat-hook is available")
    command = shlex.quote(str(executable.absolute()))
    homes = args.claude_home or [Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home() / ".claude"))]
    if args.dry_run:
        target = args.codex_home / "hooks.json"
        existing = json.loads(target.read_text()) if target.exists() else {}
        print(json.dumps(merge_hooks(existing, command, args.remove_hooks), indent=2))
        return
    target = install(args.codex_home, homes, command, args.remove_hooks)
    if args.remove_hooks:
        print(json.dumps({"hooks": str(target), "removed": True, "trust_changed": False}))
        return
    with CodexConfig(args.codex_home) as client:
        hooks = review_hooks(client, args.codex_home, command)
        digest = review_digest(hooks)
        if all(h["trustStatus"] == "trusted" for h in hooks):
            print(json.dumps({"ready": True, "trust_changed": False, "next": "Ask either agent to connect to the other by name. Terminals opened before installation need one restart/resume; new sessions are automatic."}))
            return
        if args.review or (not args.approve_digest and not sys.stdin.isatty()):
            print(json.dumps({"ready": False, "trust_changed": False, "consent_digest": digest,
                "purpose": "Register Codex presence; deliver invited peer messages during work; wake idle sessions through Codex's queue.",
                "hooks": hooks, "next": "Review this manifest. Run peer-chat setup interactively, or pass --approve-digest only after explicit owner approval."}, indent=2))
            return
        if not args.approve_digest:
            print("Peer Chat will enable local session discovery, delivery during work, and idle wake-up.")
            print("No model launches, API keys, or changes to sandbox/approval settings.")
            choice = input("Enable Peer Chat? [y/N/details] ").strip().lower()
            if choice == "details":
                print("These exact definitions use Codex's version-sensitive hook trust configuration:")
                for hook in hooks:
                    print(f"  {hook['eventName']}: {hook['command']} [{hook['currentHash']}]")
                choice = input("Enable these reviewed definitions? [y/N] ").strip().lower()
            if choice not in ("y", "yes"):
                print("Not enabled. Your existing settings are unchanged; setup can be resumed later.")
                return
            args.approve_digest = digest
        enable_reviewed(client, args.codex_home, command, args.approve_digest)
    print(json.dumps({"ready": True, "trust_changed": True,
        "next": "Setup complete. Ask either agent to connect to the other by name. Terminals opened before installation need one restart/resume; new sessions are automatic."}))


if __name__ == "__main__":
    main()
