#!/usr/bin/env bash
# Postgres + Debezium CDC competitor benchmark (Pattern P2). Runs the same insert workload as
# `cascade compare-cdc`, registers a Debezium connector, and counts the CDC events Debezium
# captured on the Kafka topic. Emits results/compare_postgres_debezium.json.
#
#   docker compose -f docker/compare/docker-compose.yml up -d   # wait ~30-60s for Connect
#   ./docker/compare/bench-pg-debezium.sh [N]
#
# SCAFFOLD: validate on Linux + tune to your Debezium 2.7 topic naming before quoting numbers.
set -euo pipefail
N="${1:-50000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PGURL="postgresql://bench:bench@localhost:5432/bench"
CONNECT="http://localhost:8083"
TOPIC="bench.public.patents"          # <server>.<schema>.<table> per Debezium defaults

command -v psql  >/dev/null || { echo "need psql (postgresql-client)"; exit 1; }
command -v jq    >/dev/null || { echo "need jq"; exit 1; }

echo "[1/5] wait for Postgres + Kafka Connect"
for i in $(seq 1 60); do pg_isready -d "$PGURL" >/dev/null 2>&1 && break; sleep 1; done
for i in $(seq 1 60); do curl -sf "$CONNECT/connectors" >/dev/null 2>&1 && break; sleep 1; done

echo "[2/5] create table"
psql "$PGURL" -q -c "DROP TABLE IF EXISTS patents;" \
  -c "CREATE TABLE patents(patent_id TEXT PRIMARY KEY, title TEXT, abstract TEXT, assignee TEXT,
       grant_date TEXT, grant_year INT, claims_len INT, n_cpc INT, n_citations INT);"

echo "[3/5] register Debezium connector"
curl -sf -X POST "$CONNECT/connectors" -H 'Content-Type: application/json' -d '{
  "name": "patents-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres", "database.port": "5432",
    "database.user": "bench", "database.password": "bench", "database.dbname": "bench",
    "topic.prefix": "bench", "table.include.list": "public.patents",
    "plugin.name": "pgoutput", "slot.name": "bench_slot"
  }}' >/dev/null
sleep 5

echo "[4/5] run $N inserts (timed)"
START=$(date +%s.%N)
psql "$PGURL" -q <<SQL
BEGIN;
INSERT INTO patents
SELECT 'P'||g, 'title '||g, 'abstract '||g, 'assignee '||(g%1000), '2020-01-01',
       2020, (g%50)+1, (g%8)+1, g%30
FROM generate_series(1,$N) g;
COMMIT;
SQL
END=$(date +%s.%N)
SECS=$(echo "$END - $START" | bc)
RPS=$(echo "$N / $SECS" | bc)

echo "[5/5] count CDC events Debezium captured on $TOPIC"
EVENTS=$(docker compose -f "$ROOT/docker/compare/docker-compose.yml" exec -T kafka \
  /kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic "$TOPIC" 2>/dev/null \
  | awk -F: '{s+=$3} END{print s+0}')
PGBYTES=$(psql "$PGURL" -t -A -c "SELECT pg_total_relation_size('patents');" | tr -d '[:space:]')

mkdir -p "$ROOT/results"
cat > "$ROOT/results/compare_postgres_debezium.json" <<JSON
{
  "_name": "compare_postgres_debezium",
  "stack": "postgres_debezium",
  "rows": $N,
  "ingest_seconds": $SECS,
  "rows_per_sec": $RPS,
  "cdc_events_captured": ${EVENTS:-0},
  "storage_bytes": ${PGBYTES:-0},
  "notes": "Postgres logical decoding + Debezium 2.7 + Kafka. Compare vs results/compare_cdc.json turso_builtin_cdc."
}
JSON
echo "wrote results/compare_postgres_debezium.json"
