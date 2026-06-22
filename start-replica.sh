#!/usr/bin/env bash
# One-command Cascade REPLICA (edge) startup: build if needed, ensure Ollama+model, launch the
# telemetry agent, then continuously pull+search so the node shows live on the fleet dashboard.
#
#   ./start-replica.sh [config]          # default: configs/replica.toml
#
# env:
#   REMOTE_URL        override sync.remote_url (else uses the config's value)
#   REPLICA_QUERY     the query to run in the loop (default: a science/tech prompt)
#   REPLICA_INTERVAL  seconds between searches (default 15; 0 = one search then stop)
#   AGENT_PORT        telemetry agent port (default 7071)
# Runs on macOS / Linux. On a Mac edge, Ollama is local (localhost:11434).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TEO_REPO_ROOT="$ROOT"
cd "$ROOT"
CONFIG="${1:-configs/replica.toml}"
[ -n "${REMOTE_URL:-}" ] && export TURSO_REMOTE_URL="$REMOTE_URL"   # harmless if unused by config

# 1. Rust toolchain
if ! command -v cargo >/dev/null 2>&1 && [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi
if ! command -v cargo >/dev/null 2>&1; then
  echo ">> installing rustup (one-time)…"
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && . "$HOME/.cargo/env"
fi

# 2. Build if needed
if [ ! -x target/release/cascade ]; then
  echo ">> building cascade (first run)…"
  ./setup.sh || cargo build --release
fi

# 3. Ollama (local on the edge — embeds the query)
EMB_URL="${CASCADE_EMBED_URL:-http://localhost:11434}"
if ! curl -sf "$EMB_URL/api/tags" >/dev/null 2>&1; then
  echo "!! Ollama not reachable at $EMB_URL — start it (ollama serve), then re-run." >&2
  exit 1
fi
if ! curl -s "$EMB_URL/api/tags" | grep -q all-minilm; then
  echo ">> pulling all-minilm…"
  ollama pull all-minilm 2>/dev/null || curl -s "$EMB_URL/api/pull" -d '{"name":"all-minilm"}' >/dev/null || true
fi

mkdir -p .work/logs
LOG=".work/logs/replica.log"; : > "$LOG"

# 4. Telemetry agent
if command -v python3 >/dev/null 2>&1; then
  AGENT_PORT="${AGENT_PORT:-7071}"
  python3 tools/agent.py --role replica --config "$CONFIG" --log "$LOG" --port "$AGENT_PORT" --bind 0.0.0.0 >/dev/null 2>&1 &
  AGENT_PID=$!
  trap 'kill $AGENT_PID 2>/dev/null' EXIT INT TERM
  echo ">> telemetry agent on :$AGENT_PORT"
fi

# this edge's LAN IP (Linux: hostname -I; macOS: ipconfig getifaddr)
IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
[ -z "${IP:-}" ] && IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"
echo ">> replica live. Add this node to the dashboard:  http://${IP:-<this-ip>}:${AGENT_PORT:-7071}"

# 5. Continuous pull+search so the edge shows end-to-end activity on the dashboard
Q="${REPLICA_QUERY:-recent changes about science and technology}"
INT="${REPLICA_INTERVAL:-15}"
echo ">> searching for: \"$Q\""
echo
if [ "$INT" = "0" ]; then
  ./target/release/cascade search "$Q" 5 --config "$CONFIG" 2>&1 | tee -a "$LOG"
else
  echo ">> looping every ${INT}s (Ctrl-C to stop)"
  while true; do
    ./target/release/cascade search "$Q" 5 --config "$CONFIG" 2>&1 | tee -a "$LOG"
    echo "---- $(date '+%H:%M:%S') ----" | tee -a "$LOG"
    sleep "$INT"
  done
fi
