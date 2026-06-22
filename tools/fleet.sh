#!/usr/bin/env bash
# One-command fleet health — pulls /fleet-telemetry from a master agent and prints a compact
# per-node summary (role, up, key metrics, last error, last log line). Built for fast debugging:
# one call shows the whole master<->replica picture.
#   tools/fleet.sh [master-agent-url]      # default http://localhost:7071
python3 - "${1:-http://localhost:7071}" <<'PY'
import sys, json, urllib.request
url = sys.argv[1].rstrip("/") + "/fleet-telemetry"
try:
    with urllib.request.urlopen(url, timeout=10) as r:
        d = json.load(r)
except Exception as e:
    print("could not read", url, ":", e); sys.exit(1)
nodes = d.get("nodes", [])
print(f"fleet: {len(nodes)} node(s)  (via {url})")
for n in nodes:
    if not n.get("__ok"):
        print(f"  x OFFLINE {n.get('__url')} -- {n.get('__err','')}"); continue
    m = n.get("metrics", {}); role = n.get("role", "?"); host = n.get("host", "?")
    print(f"  [{'UP ' if n.get('up') else 'idle'}] {role:<8} {host:<16} {n.get('__url')}")
    if role == "replica":
        print(f"      searches={m.get('searches',0)} last_retrieval_ms={m.get('last_retrieval_ms')} last_pull={m.get('last_pull_changed')}")
    else:
        print(f"      stored={m.get('stored',0)} pushed={m.get('pushed_batches',0)} "
              f"push_failed={m.get('push_failed',0)} put_failed={m.get('put_failed',0)} reconnects={m.get('reconnects',0)}")
    errs = n.get("errors", [])
    if errs: print(f"      last error: {errs[-1][:140]}")
    lg = n.get("recent_log", [])
    if lg: print(f"      last log:   {lg[-1][:140]}")
PY
