#!/usr/bin/env bash
# Post-build test suite. Run after `cargo build` to gate that the master/replica config flows
# still work end to end — so a build never breaks how users pass configs to the library/CLI.
#
#   ./setup.sh && ollama pull all-minilm && ./test.sh
#
# Phases: (1) config contract  (2) health  (3) master role  (4) replica role  (5) verdict.
# Exits non-zero if any phase fails. Needs: tursodb CLI (./setup.sh) + Ollama with all-minilm.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TEO_REPO_ROOT="$ROOT"
cd "$ROOT"

PASS=0; FAIL=0
ok()   { echo "   ✓ $*"; PASS=$((PASS+1)); }
bad()  { echo "   ✗ $*"; FAIL=$((FAIL+1)); }

BIN="$ROOT/target/release/cascade"; [ -x "$BIN" ] || BIN="$ROOT/target/debug/cascade"

# ---- 1. config contract (no infra) ----
echo "[1/5] config contract"
if cargo test --test config_cases --quiet >/tmp/teo_cfg_test.log 2>&1; then
  ok "config_cases ($(grep -oE '[0-9]+ passed' /tmp/teo_cfg_test.log | head -1))"
else
  bad "config_cases failed"; sed -n '1,40p' /tmp/teo_cfg_test.log
fi

# ---- 2. health ----
echo "[2/5] health"
[ -x "$BIN" ] && ok "cascade binary built" || bad "cascade binary missing (cargo build)"
TURSODB="$(find .work/bin -name tursodb -type f 2>/dev/null | head -1)"
[ -n "$TURSODB" ] && ok "tursodb CLI present" || bad "tursodb CLI missing (./setup.sh)"
OLLAMA_OK=0
if [ -n "${CASCADE_FAKE_EMBED:-}" ] && [ "${CASCADE_FAKE_EMBED}" != "0" ]; then
  ok "fake embedder (CASCADE_FAKE_EMBED) — skipping Ollama checks"; OLLAMA_OK=1
elif curl -sf http://localhost:11434/api/tags >/dev/null 2>&1; then
  ok "ollama reachable"
  if curl -s http://localhost:11434/api/tags | grep -q all-minilm; then ok "all-minilm present"; OLLAMA_OK=1
  else bad "all-minilm missing (ollama pull all-minilm)"; fi
else bad "ollama not reachable (ollama serve)"; fi

# can't run the runtime phases without these
if [ -z "$TURSODB" ] || [ ! -x "$BIN" ] || [ "$OLLAMA_OK" -ne 1 ]; then
  echo; echo "RESULT: $PASS passed, $FAIL failed — health prerequisites missing, skipped runtime phases."
  exit 1
fi

echo "== clean state =="
rm -f .work/db/smoke_hub.db .work/db/smoke_hub.db-* .work/db/local_master.db .work/db/local_master.db-* \
      .work/db/local_replica.db .work/db/local_replica.db-* .work/olap/local.duckdb 2>/dev/null || true
mkdir -p .work/db .work/olap
"$TURSODB" .work/db/smoke_hub.db --sync-server 127.0.0.1:8080 >/tmp/teo_test_hub.log 2>&1 &
HUB=$!
trap 'kill $HUB 2>/dev/null || true' EXIT
for i in $(seq 1 40); do nc -z 127.0.0.1 8080 2>/dev/null && break; sleep 0.3; done
nc -z 127.0.0.1 8080 2>/dev/null && ok "sync hub up" || { bad "hub failed to start"; cat /tmp/teo_test_hub.log; }

# ---- 3. master role: mock event flow (embed + CDC + push + drain) ----
echo "[3/5] master role (mock event flow)"
SERVE_OUT="$("$BIN" serve --config configs/local-master.toml 2>&1)"
echo "$SERVE_OUT" | grep -q "source done: 6" && ok "ingested+embedded 6 mock events" || bad "master did not ingest 6 events"
ROWS="$(echo "$SERVE_OUT" | grep -oE '\([0-9]+ rows\)' | grep -oE '[0-9]+' | tail -1)"
[ "${ROWS:-0}" -ge 6 ] && ok "CDC drained to OLAP DuckDB ($ROWS rows)" || bad "CDC->OLAP drain rows=${ROWS:-0} (<6)"

# ---- 4. replica role: pull + co-located vector search ----
echo "[4/5] replica role (pull + search)"
SEARCH_OUT="$("$BIN" search "what replaces kafka and debezium" 3 --config configs/local-replica.toml 2>&1)"
echo "$SEARCH_OUT" | grep -q "Sources" && ok "replica pulled + returned sources" || bad "replica returned no sources"
echo "$SEARCH_OUT" | grep -q "retrieval" && ok "ran co-located vector retrieval" || bad "no vector retrieval"
echo "$SEARCH_OUT" | grep -qi "kafka\|debezium" && ok "top-k semantically correct (Kafka/Debezium doc)" \
  || bad "expected the Kafka/Debezium doc in results"

kill $HUB 2>/dev/null || true

# ---- 5. verdict ----
echo
echo "[5/5] RESULT: $PASS passed, $FAIL failed"
if [ "$FAIL" -eq 0 ]; then
  echo "PASS — master + replica each performed their roles; config flow intact."
  exit 0
else
  echo "FAIL — a role or config flow regressed."
  exit 1
fi
