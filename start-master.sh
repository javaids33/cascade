#!/usr/bin/env bash
# One-command Cascade MASTER (producer) startup: ensure the Rust toolchain, build if needed, wire
# the embedder, print the LAN address for the edge, then serve (spawns the sync hub + ingest + CDC
# + push per the config).
#
#   ./start-master.sh [config]          # default: configs/master.toml
#   CASCADE_BUILD_ONLY=1 ./start-master.sh   # build the toolchain + binary, then stop (no serve)
#
# Runs on Linux / WSL2 / macOS. On a Windows PC, double-click start-master.cmd (it sets up the
# Windows<->WSL networking + Ollama bridge, then calls this inside WSL).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TEO_REPO_ROOT="$ROOT"
cd "$ROOT"
CONFIG="${1:-configs/master.toml}"

is_wsl() { grep -qi microsoft /proc/version 2>/dev/null; }

# ---------------------------------------------------------------------------
# 1. Rust toolchain (Blocker 1: a fresh WSL has no cargo). Install rustup once.
# ---------------------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1 && [ -f "$HOME/.cargo/env" ]; then
  . "$HOME/.cargo/env"
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo ">> Rust toolchain not found — installing rustup (one-time, ~1-2 min)…"
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
  . "$HOME/.cargo/env"
fi
echo ">> cargo: $(cargo --version)"

# ---------------------------------------------------------------------------
# 1b. System build deps: DuckDB (bundled) + Iceberg + openssl need a C/C++ toolchain and headers.
#     A fresh WSL/Ubuntu has none ("linker `cc` not found"). start-master.cmd installs these as
#     root before calling us; this is the fallback for a direct/native run.
# ---------------------------------------------------------------------------
if ! command -v cc >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    echo ">> Installing system build deps (build-essential, libssl-dev, libclang-dev, cmake)…"
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential pkg-config libssl-dev libclang-dev cmake
  else
    echo "!! No C toolchain (cc) found. Install it once, then re-run:" >&2
    echo "   sudo apt-get install -y build-essential pkg-config libssl-dev libclang-dev cmake   (Debian/Ubuntu)" >&2
    echo "   xcode-select --install                                                             (macOS)" >&2
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# 2. Build the binary if missing (setup.sh also downloads the tursodb CLI).
# ---------------------------------------------------------------------------
if [ ! -x target/release/cascade ]; then
  echo ">> Building cascade (first run: a few minutes for DuckDB + Iceberg)…"
  ./setup.sh
fi

if [ "${CASCADE_BUILD_ONLY:-0}" = "1" ]; then
  echo ">> build-only: cascade is built at target/release/cascade — stopping before serve."
  exit 0
fi

# ---------------------------------------------------------------------------
# 3. Embedder bridge (Blocker 2): in WSL the master runs in Linux but Ollama runs on the Windows
#    host, so the config's localhost:11434 points at WSL (no Ollama there). Redirect embedding +
#    generation to the Windows host unless the caller already set CASCADE_EMBED_URL.
# ---------------------------------------------------------------------------
if is_wsl && [ -z "${CASCADE_EMBED_URL:-}" ]; then
  HOSTIP="$(ip route show default | awk '{print $3}' | head -1)"
  if [ -n "$HOSTIP" ]; then
    export CASCADE_EMBED_URL="http://$HOSTIP:11434"
    echo ">> WSL detected — embedding via the Windows host Ollama at $CASCADE_EMBED_URL"
  fi
fi
EMB_URL="${CASCADE_EMBED_URL:-http://localhost:11434}"

# Make sure the embedder is reachable + has the model; give the exact fix if not.
if curl -sf "$EMB_URL/api/tags" >/dev/null 2>&1; then
  if ! curl -s "$EMB_URL/api/tags" | grep -q all-minilm; then
    echo ">> pulling embedding model all-minilm via $EMB_URL …"
    curl -s "$EMB_URL/api/pull" -d '{"name":"all-minilm"}' >/dev/null || true
  fi
else
  echo "!! Ollama not reachable at $EMB_URL — the master cannot embed, so it would crash on the"
  echo "   first event. Fix this first (failing fast instead of serving):"
  if is_wsl; then
    echo "   The WSL->host portproxy isn't in place. Run start-master.cmd (it forwards the host"
    echo "   gateway :11434 -> 127.0.0.1:11434 + opens the firewall). Also confirm Ollama is running"
    echo "   on Windows: curl http://localhost:11434/api/tags"
  else
    echo "   Start it with:  ollama serve   (and: ollama pull all-minilm)"
  fi
  exit 1
fi

# ---------------------------------------------------------------------------
# 4. Address banner + serve.
# ---------------------------------------------------------------------------
echo
echo "Master serves the sync hub on 0.0.0.0:8080."
if is_wsl; then
  echo "(WSL internal IP $(hostname -I 2>/dev/null | awk '{print $1}') — the edge uses the PC's LAN IP,"
  echo " which start-master.cmd prints and bridges to WSL via portproxy.)"
else
  IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  [ -n "${IP:-}" ] && echo "On the edge: set configs/replica.toml -> sync.remote_url = http://$IP:8080"
  [ -n "${IP:-}" ] && echo "Preflight from the edge:  curl http://$IP:8080/   (expect HTTP 404, not a timeout)"
fi
echo "Ctrl-C to stop."
echo

# Telemetry agent (best-effort): exposes config + logs + metrics over HTTP for the dashboard.
mkdir -p .work/logs
LOGFILE=".work/logs/master.log"
: > "$LOGFILE"
if command -v python3 >/dev/null 2>&1; then
  AGENT_PORT="${AGENT_PORT:-7071}"
  python3 tools/agent.py --role master --config "$CONFIG" --log "$LOGFILE" --port "$AGENT_PORT" --bind 0.0.0.0 >/dev/null 2>&1 &
  AGENT_PID=$!
  trap 'kill $AGENT_PID 2>/dev/null' EXIT INT TERM
  echo ">> telemetry agent on :$AGENT_PORT  (open tools/dashboard.html; node URL http://<this-ip>:$AGENT_PORT)"
fi

# Serve, mirroring output to the log the agent reads (so `exec` is replaced by a tee pipeline).
target/release/cascade serve --config "$CONFIG" 2>&1 | tee "$LOGFILE"
