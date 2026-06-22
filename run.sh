#!/usr/bin/env bash
# Run the full harness end to end. Builds the release binary if needed, then `cascade run-all`.
#
#   ./run.sh                 # synthetic data (default 50k patents, or env SYNTH_N)
#   ./run.sh 20000           # smaller synthetic run
#   PATENTS_JSONL=/path/to.jsonl ./run.sh   # real data
#
# Individual phases:  ./target/release/cascade <phase>   (gen-synthetic|cdc-overhead|cdc-to-olap|
#                     replication|olap|vector|report).  See `cascade --help`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TEO_REPO_ROOT="$REPO_ROOT"

BIN="$REPO_ROOT/target/release/cascade"
if [ ! -x "$BIN" ]; then
  echo "Building cascade (cargo build --release)..."
  ( cd "$REPO_ROOT" && cargo build --release )
fi

exec "$BIN" run-all "$@"
