#!/bin/sh
# Install into the current user's account, then run one guided consent flow.
set -eu
peer_source=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Share bundles contain a prebuilt wheel; source checkouts remain installable.
peer_package="$peer_source"
peer_wheel_found=false
for peer_wheel in "$peer_source"/local_peer_chat-*-py3-none-any.whl; do
    [ -f "$peer_wheel" ] || continue
    if [ "$peer_wheel_found" = true ]; then
        echo 'More than one Peer Chat wheel found; extract a fresh share bundle.' >&2
        exit 1
    fi
    peer_package="$peer_wheel"
    peer_wheel_found=true
done
if ! command -v codex >/dev/null 2>&1 || ! command -v claude >/dev/null 2>&1; then
    echo 'Install Codex and Claude Code first, then run this installer again.' >&2
    exit 1
fi
if command -v uv >/dev/null 2>&1; then
    uv tool install --force "$peer_package"
    peer_bin=$(uv tool dir --bin)
    exec "$peer_bin/peer-chat" setup "$@"
elif command -v pipx >/dev/null 2>&1; then
    pipx install --force "$peer_package"
    peer_bin=$(pipx environment --value PIPX_BIN_DIR)
    exec "$peer_bin/peer-chat" setup "$@"
elif command -v python3 >/dev/null 2>&1; then
    peer_venv="${XDG_DATA_HOME:-$HOME/.local/share}/peer-chat/venv"
    python3 -m venv "$peer_venv"
    "$peer_venv/bin/python" -m pip install --upgrade "$peer_package"
    mkdir -p "$HOME/.local/bin"
    for peer_command in peer-chat peer-chat-hook peer-chat-setup; do
        peer_target="$HOME/.local/bin/$peer_command"
        if [ -e "$peer_target" ] && [ ! -L "$peer_target" ]; then
            echo "Existing executable at $peer_target; refusing to overwrite it." >&2
            exit 1
        fi
        ln -sfn "$peer_venv/bin/$peer_command" "$peer_target"
    done
    exec "$peer_venv/bin/peer-chat" setup "$@"
else
    echo 'Python 3.10+, uv, or pipx is required to install this package.' >&2
    exit 1
fi
