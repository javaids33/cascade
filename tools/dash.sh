#!/usr/bin/env bash
# Cascade terminal dashboard — live-tails telemetry from one or more agent URLs.
#   tools/dash.sh http://localhost:7071 http://192.168.1.50:7071
# Defaults to http://localhost:7071 (the local master agent).
set -u
NODES=("$@"); [ ${#NODES[@]} -eq 0 ] && NODES=("http://localhost:7071")

fmt() {  # reads telemetry JSON on stdin, prints a compact block
  python3 - "$1" <<'PY'
import sys, json
url = sys.argv[1]
try:
    d = json.load(sys.stdin)
except Exception:
    print(f"  \033[31m● OFFLINE\033[0m  {url}"); sys.exit()
m = d.get("metrics", {}); c = (d.get("config") or {}).get("parsed", {})
up = d.get("up"); dot = "\033[32m●\033[0m" if up else "\033[33m●\033[0m"
node = c.get("node", {}); sync = c.get("sync", {}); emb = c.get("embedding", {}); src = c.get("source", {})
print(f"  {dot} \033[1m{d.get('role','?').upper()}\033[0m  {d.get('host','?')}  ({url})")
print(f"      cfg: source={src.get('kind','-')} model={emb.get('model','-')}/{emb.get('dim','-')} "
      f"bind={sync.get('bind','-')} remote={sync.get('remote_url','-')}")
pf = m.get("push_failed", 0)
pf_s = f"\033[31m{pf}\033[0m" if pf else "0"
print(f"      stored={m.get('stored',0)} rate={m.get('rate_per_s',0)}/s pushed={m.get('pushed_batches',0)} "
      f"push_failed={pf_s} put_failed={m.get('put_failed',0)} reconnects={m.get('reconnects',0)}")
errs = d.get("errors", [])
if errs:
    print(f"      \033[31mlast error:\033[0m {errs[-1][:120]}")
log = d.get("recent_log", [])
if log:
    print(f"      log: {log[-1][:120]}")
PY
}

while true; do
  clear
  echo "⛰  CASCADE fleet — $(date '+%H:%M:%S')   (Ctrl-C to quit)"
  echo "------------------------------------------------------------------"
  for u in "${NODES[@]}"; do
    body="$(curl -s --max-time 3 "${u%/}/telemetry" 2>/dev/null)"
    echo "$body" | fmt "$u"
    echo
  done
  sleep 2
done
