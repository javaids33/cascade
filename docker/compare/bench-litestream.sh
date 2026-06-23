#!/usr/bin/env bash
# SQLite + Litestream replication competitor benchmark (Pattern P1). Ingests N rows into a SQLite
# db, replicates with Litestream to a local target, then restores to a fresh db and times the
# bootstrap + measures bytes. Compare vs Cascade native sync in results/replication.json.
#
#   ./docker/compare/bench-litestream.sh [N]
#
# SCAFFOLD: needs `sqlite3` + Docker (uses the litestream/litestream image). Validate on Linux.
set -euo pipefail
N="${1:-50000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$ROOT/.work/compare/litestream"
command -v sqlite3 >/dev/null || { echo "need sqlite3"; exit 1; }
rm -rf "$WORK"; mkdir -p "$WORK/replica"
SRC="$WORK/source.db"; DST="$WORK/restored.db"

echo "[1/4] ingest $N rows into SQLite (timed)"
START=$(date +%s.%N)
sqlite3 "$SRC" <<SQL
CREATE TABLE patents(patent_id TEXT PRIMARY KEY, title TEXT, abstract TEXT, assignee TEXT,
  grant_date TEXT, grant_year INT, claims_len INT, n_cpc INT, n_citations INT);
WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM g WHERE x<$N)
INSERT INTO patents SELECT 'P'||x,'title '||x,'abstract '||x,'assignee '||(x%1000),
  '2020-01-01',2020,(x%50)+1,(x%8)+1,x%30 FROM g;
SQL
END=$(date +%s.%N); SECS=$(echo "$END - $START" | bc); RPS=$(echo "$N / $SECS" | bc)

echo "[2/4] litestream replicate -> $WORK/replica"
docker run --rm -v "$WORK:/data" litestream/litestream:latest \
  replicate -exec "sleep 3" /data/source.db file:///data/replica || true

echo "[3/4] litestream restore (timed bootstrap)"
RSTART=$(date +%s.%N)
docker run --rm -v "$WORK:/data" litestream/litestream:latest \
  restore -o /data/restored.db file:///data/replica
REND=$(date +%s.%N); RSECS=$(echo "$REND - $RSTART" | bc)

echo "[4/4] verify + size"
ROWS=$(sqlite3 "$DST" "SELECT COUNT(*) FROM patents;" 2>/dev/null || echo 0)
BYTES=$(du -sb "$WORK/replica" | awk '{print $1}')
mkdir -p "$ROOT/results"
cat > "$ROOT/results/compare_sqlite_litestream.json" <<JSON
{
  "_name": "compare_sqlite_litestream",
  "stack": "sqlite_litestream",
  "rows": $N,
  "ingest_seconds": $SECS,
  "rows_per_sec": $RPS,
  "replica_bootstrap_restore_sec": $RSECS,
  "restored_rows": ${ROWS:-0},
  "replica_storage_bytes": ${BYTES:-0},
  "notes": "Litestream WAL shipping to a file target. Compare vs results/replication.json (native push/pull)."
}
JSON
echo "wrote results/compare_sqlite_litestream.json"
