#!/usr/bin/env bash
# Install bsc-exporter as a systemd timer unit.
# Run as root: sudo ./install.sh

set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-/opt/bsc-exporter}"
CONFIG_DIR="${CONFIG_DIR:-/etc/bsc-exporter}"
STATE_DIR="${STATE_DIR:-/var/lib/bsc-exporter}"
LOG_DIR="${LOG_DIR:-/var/log/bsc-exporter}"
USER_NAME="${USER_NAME:-bsc}"
PYTHON="${PYTHON:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(dirname "$SCRIPT_DIR")"

if [[ $EUID -ne 0 ]]; then
    echo "Must run as root (sudo)." >&2
    exit 1
fi

# Verify Python >= 3.10 (3.6/3.7 on RHEL 7 don't have pyarrow wheels;
# 3.11+ needs a parsimonious workaround but is supported).
if ! "$PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
    PY_VER=$("$PYTHON" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))' 2>/dev/null || echo "unknown")
    cat >&2 <<EOF
Need Python 3.10+ (3.10 strongly preferred). Found $PY_VER at $(command -v "$PYTHON" 2>/dev/null || echo "$PYTHON").

Set PYTHON env var, e.g.:
  sudo PYTHON=/root/.pyenv/versions/3.10.15/bin/python ./systemd/install.sh

If the path is under /root/ (pyenv default), grant traversal first:
  sudo chmod o+x /root /root/.pyenv /root/.pyenv/versions
  sudo chmod -R o+rX /root/.pyenv/versions/3.10.15
EOF
    exit 1
fi
PY_VER=$("$PYTHON" -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "==> Using Python $PY_VER at $(command -v "$PYTHON")"

# 1. Service user
if ! id -u "$USER_NAME" >/dev/null 2>&1; then
    echo "==> Creating service user: $USER_NAME"
    useradd --system --no-create-home --shell /usr/sbin/nologin "$USER_NAME"
fi

# 2. Directories
echo "==> Creating directories"
install -d -o "$USER_NAME" -g "$USER_NAME" -m 0755 \
    "$INSTALL_DIR" "$CONFIG_DIR" "$STATE_DIR" "$LOG_DIR"

# 3. Copy code
echo "==> Copying source to $INSTALL_DIR"
install -o "$USER_NAME" -g "$USER_NAME" -m 0755 "$SOURCE_DIR/exporter.py" "$INSTALL_DIR/"
install -o "$USER_NAME" -g "$USER_NAME" -m 0644 "$SOURCE_DIR/requirements.txt" "$INSTALL_DIR/"

# 4. Python venv
if [[ ! -x "$INSTALL_DIR/venv/bin/python" ]]; then
    echo "==> Creating venv at $INSTALL_DIR/venv (using $PYTHON)"
    sudo -u "$USER_NAME" "$PYTHON" -m venv "$INSTALL_DIR/venv"
fi
echo "==> Installing dependencies"
sudo -u "$USER_NAME" "$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
sudo -u "$USER_NAME" "$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"
# Python 3.11+ needs parsimonious >=0.10 (eth-abi 2.x pins <0.9 but old
# parsimonious uses inspect.getargspec, removed in 3.11). Force-install
# bypassing the dep pin — see AGENTS.md for context.
if "$INSTALL_DIR/venv/bin/python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
    echo "==> Python 3.11+: forcing parsimonious>=0.10.4"
    sudo -u "$USER_NAME" "$INSTALL_DIR/venv/bin/pip" install --quiet --no-deps 'parsimonious>=0.10.4'
fi

# 5. Config (skip if already present)
if [[ ! -f "$CONFIG_DIR/config.yaml" ]]; then
    echo "==> Writing $CONFIG_DIR/config.yaml from example"
    install -o root -g "$USER_NAME" -m 0640 \
        "$SOURCE_DIR/config.yaml.example" "$CONFIG_DIR/config.yaml"
    echo "    !! Edit $CONFIG_DIR/config.yaml before enabling the timer."
else
    echo "==> Keeping existing $CONFIG_DIR/config.yaml"
fi

# 6. Make work_dir match STATE_DIR by default
echo "==> Note: ensure config.yaml's export.work_dir points to $STATE_DIR"

# 7. systemd units
echo "==> Installing systemd units"
install -m 0644 "$SCRIPT_DIR/bsc-exporter.service" /etc/systemd/system/
install -m 0644 "$SCRIPT_DIR/bsc-exporter.timer"   /etc/systemd/system/
systemctl daemon-reload

cat <<EOF

✅ Install complete.

Next steps:
  1. Edit $CONFIG_DIR/config.yaml (rpc_url, S3 bucket, etc.)
     Make sure export.work_dir = $STATE_DIR
  2. Test a one-off run:
       sudo systemctl start bsc-exporter.service
       sudo journalctl -u bsc-exporter -f
  3. Enable the daily timer:
       sudo systemctl enable --now bsc-exporter.timer
  4. Verify schedule:
       sudo systemctl list-timers bsc-exporter.timer

Backfill (one-off, bypasses the timer):
  sudo -u $USER_NAME $INSTALL_DIR/venv/bin/python $INSTALL_DIR/exporter.py \\
      --config $CONFIG_DIR/config.yaml \\
      --start 2020-08-29 --end 2026-04-29 -j 4
EOF
