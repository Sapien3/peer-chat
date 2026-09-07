import os
from pathlib import Path
import subprocess


def test_one_command_installer_forwards_to_guided_setup_with_spaces(tmp_path):
    scripts = tmp_path / "commands with spaces"
    scripts.mkdir()
    for name in ("codex", "claude"):
        f = scripts / name
        f.write_text("#!/bin/sh\nexit 0\n")
        f.chmod(0o755)
    log = tmp_path / "calls"
    uv = scripts / "uv"
    uv.write_text('#!/bin/sh\nif [ "$2" = "install" ]; then exit 0; fi\nif [ "$2" = "dir" ]; then printf "%s\\n" "$PEER_TEST_BIN"; exit 0; fi\nexit 1\n')
    uv.chmod(0o755)
    command = scripts / "peer-chat"
    command.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$PEER_TEST_LOG"\n')
    command.chmod(0o755)
    script = Path(__file__).resolve().parents[1] / "install.sh"
    env = dict(os.environ, PATH=str(scripts) + ":/usr/bin:/bin", PEER_TEST_BIN=str(scripts), PEER_TEST_LOG=str(log))
    result = subprocess.run(["sh", str(script), "--review"], env=env, cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert log.read_text().splitlines() == ["setup", "--review"]
