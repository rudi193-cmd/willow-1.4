#!/usr/bin/env bash
# willow-wsl.sh — Start Willow from WSL2 (Windows Subsystem for Linux)
#
# Usage:
#   bash willow-wsl.sh          # start everything
#   bash willow-wsl.sh stop     # stop everything
#   bash willow-wsl.sh status   # show running services
#
# Paths auto-detected relative to this script. Expects:
#   <this-repo>/            willow-1.4 (Shiva journal server)
#   <this-repo>/../Willow/  main Willow server + daemons

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WILLOW_DIR="$(dirname "$SCRIPT_DIR")/Willow"
W14_DIR="$SCRIPT_DIR"

# Venv: prefer ~/.willow-venv, fall back to ./venv
if   [ -f "$HOME/.willow-venv/bin/python" ]; then VENV="$HOME/.willow-venv"
elif [ -f "$W14_DIR/venv/bin/python" ];       then VENV="$W14_DIR/venv"
elif [ -f "$WILLOW_DIR/venv/bin/python" ];    then VENV="$WILLOW_DIR/venv"
else
    echo "[ERROR] No venv found. Run one of:"
    echo "        python3 -m venv ~/.willow-venv && ~/.willow-venv/bin/pip install -r $WILLOW_DIR/requirements.base.txt"
    exit 1
fi

PYTHON="$VENV/bin/python"
PID_DIR="/tmp/willow-wsl"
LOG_DIR="$PID_DIR/logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

# ── Helpers ───────────────────────────────────────────────────────────────────

pid_file() { echo "$PID_DIR/$1.pid"; }

start_daemon() {
    local name="$1" dir="$2" cmd="$3" log="$LOG_DIR/$1.log"
    local pf; pf="$(pid_file "$name")"
    if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        echo "      already running (PID $(cat "$pf"))"
        return
    fi
    cd "$dir"
    # shellcheck disable=SC2086
    nohup $cmd > "$log" 2>&1 &
    echo $! > "$pf"
    echo "      PID $! — $log"
}

stop_daemon() {
    local name="$1" pf
    pf="$(pid_file "$name")"
    if [ -f "$pf" ]; then
        local pid; pid="$(cat "$pf")"
        kill "$pid" 2>/dev/null && echo "  $name stopped" || echo "  $name already gone"
        rm -f "$pf"
    fi
}

wait_http() {
    local url="$1" label="$2" tries="${3:-20}"
    echo "      Waiting for $label..."
    for i in $(seq 1 "$tries"); do
        sleep 2
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "      OK. $url"
            return 0
        fi
    done
    echo "[FAIL] $label did not start after $((tries * 2))s"
    return 1
}

# ── Stop ──────────────────────────────────────────────────────────────────────

stop_all() {
    echo "[willow-wsl] Stopping all services..."
    for name in willow shiva pigeon ocr inbox mcp drive_watcher; do
        stop_daemon "$name"
    done
    pkill -f "pigeon_daemon.py"       2>/dev/null || true
    pkill -f "ocr_consumer_daemon.py" 2>/dev/null || true
    pkill -f "inbox_watcher.py"       2>/dev/null || true
    pkill -f "willow_server.py"       2>/dev/null || true
    pkill -f "pigeon_drive_watcher"   2>/dev/null || true
    rm -f "$WILLOW_DIR/.daemon_owner.pid"
    rm -f "$HOME/.willow/watcher.lock"
    echo "[willow-wsl] Done."
    exit 0
}

# ── Status ────────────────────────────────────────────────────────────────────

show_status() {
    echo ""
    echo "  Willow service status:"
    for name in willow shiva pigeon ocr inbox mcp drive_watcher; do
        local pf; pf="$(pid_file "$name")"
        if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
            printf "  %-16s UP   (PID %s)\n" "$name" "$(cat "$pf")"
        else
            printf "  %-16s DOWN\n" "$name"
        fi
    done
    echo ""
    curl -s http://127.0.0.1:8420/api/health 2>/dev/null && echo "" || echo "  API: not responding"
    exit 0
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

case "${1:-start}" in
    stop)   stop_all ;;
    status) show_status ;;
    start)  : ;;
    *) echo "Usage: $0 [start|stop|status]"; exit 1 ;;
esac

# ── Pre-flight ────────────────────────────────────────────────────────────────

echo ""
echo "  W I L L O W  (WSL)"
echo "  ___________________"
echo "  Willow dir : $WILLOW_DIR"
echo "  Venv       : $VENV"
echo ""

if [ ! -f "$WILLOW_DIR/server.py" ]; then
    echo "[ERROR] Willow server not found at $WILLOW_DIR"
    echo "        Clone the Willow repo as a sibling of this directory."
    exit 1
fi

rm -f "$WILLOW_DIR/.daemon_owner.pid"

# ── 1. Willow server (8420, 4 workers) ───────────────────────────────────────

echo "[1/6] Willow server :8420..."
start_daemon "willow" "$WILLOW_DIR" \
    "$PYTHON -m uvicorn server:app --host 0.0.0.0 --port 8420 --workers 4 --log-level info"
wait_http "http://127.0.0.1:8420/api/health" "Willow :8420"

# ── 2. Willow 1.4 journal server (2121) ──────────────────────────────────────

echo "[2/6] Journal server :2121..."
start_daemon "shiva" "$W14_DIR" \
    "$PYTHON -m uvicorn server:app --host 0.0.0.0 --port 2121 --log-level info"
sleep 3
pf="$(pid_file shiva)"
if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
    echo "      OK. http://127.0.0.1:2121"
else
    echo "      [WARN] Journal server exited early — check $LOG_DIR/shiva.log"
fi

# ── 3. Pigeon daemon ──────────────────────────────────────────────────────────

echo "[3/6] Pigeon daemon..."
start_daemon "pigeon" "$WILLOW_DIR" "$PYTHON core/pigeon_daemon.py"

# ── 4. OCR consumer daemon ────────────────────────────────────────────────────

echo "[4/6] OCR consumer..."
start_daemon "ocr" "$WILLOW_DIR" "$PYTHON core/ocr_consumer_daemon.py"

# ── 5. Inbox watcher (Ganesha notifications) ─────────────────────────────────

echo "[5/6] Inbox watcher..."
start_daemon "inbox" "$WILLOW_DIR" "$PYTHON tools/inbox_watcher.py"

# ── 6. MCP server ────────────────────────────────────────────────────────────

echo "[6/6] MCP server..."
start_daemon "mcp" "$WILLOW_DIR" "$PYTHON mcp/willow_server.py"

# ── Drive watcher (optional — needs Google Drive mounted at /mnt/c/Users/*/My Drive) ──

DRIVE_WATCHER="$WILLOW_DIR/core/pigeon_drive_watcher.py"
DRIVE_PATH_CHECK="/mnt/c/Users/Sean/My Drive"
if [ -f "$DRIVE_WATCHER" ] && [ -d "$DRIVE_PATH_CHECK" ]; then
    echo "[+]   Drive watcher..."
    start_daemon "drive_watcher" "$WILLOW_DIR" \
        "$PYTHON core/pigeon_drive_watcher.py --watch --interval 10"
else
    echo "      Drive watcher skipped (Drive not mounted or watcher not found)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "  ─────────────────────────────────────────"
echo "  Willow  : http://127.0.0.1:8420"
echo "  Shiva   : http://127.0.0.1:2121"
echo "  Journal : http://127.0.0.1:2121/journal/"
echo "  Logs    : $LOG_DIR/"
echo "  Stop    : bash willow-wsl.sh stop"
echo "  Status  : bash willow-wsl.sh status"
echo "  ─────────────────────────────────────────"
echo ""

# Note: apps/watcher.py (Drop folder ingestion) uses Windows-only paths.
# Run willow-14.bat from Windows if you need Drop folder watching.
