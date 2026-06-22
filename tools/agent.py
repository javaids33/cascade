#!/usr/bin/env python3
"""Cascade telemetry agent — a zero-dependency sidecar that exposes a node's config, logs,
metrics, and errors over HTTP so a dashboard on ANY computer can pull them. Run one per node
(master or replica), on any machine; point the dashboard at each agent's URL.

  python3 tools/agent.py --role master  --config configs/master.toml  --log .work/logs/master.log
  python3 tools/agent.py --role replica --config configs/replica.toml --log .work/logs/replica.log

Endpoints (all send Access-Control-Allow-Origin: * so the dashboard can fetch cross-machine):
  GET /health     -> {ok, role, host}
  GET /telemetry  -> {host, role, ts, up, config, metrics, recent_log[], errors[]}
  GET /config     -> parsed config
  GET /logs       -> recent raw log lines
"""
import argparse, json, os, re, socket, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import tomllib  # py3.11+
except ModuleNotFoundError:
    tomllib = None

ARGS = None
START = time.time()

STORED_RE = re.compile(r"stored=(\d+)")
RATE_RE = re.compile(r"rate=([\d.]+)/s")
RET_RE = re.compile(r"\[retrieval\]\s*([\d.]+)\s*ms")
PULL_RE = re.compile(r"pulled changed=(\w+)")
ERR_RE = re.compile(r"(ERROR|panic|push failed|put failed|connect failed|stream dropped|Error:|not reachable)", re.I)


def tail(path, n):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block, data = 8192, b""
            while size > 0 and data.count(b"\n") <= n:
                step = min(block, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
        # the binary blob lines from sync errors can be huge; cap each line
        return [ln[:600] for ln in data.decode("utf-8", "replace").splitlines()[-n:]]
    except FileNotFoundError:
        return []


def parse_metrics(lines):
    # master-side
    stored = pushed = push_failed = put_failed = reconnects = 0
    rate = 0.0
    source = ""
    # replica-side
    searches = 0
    last_retrieval_ms = None
    last_pull_changed = None
    for ln in lines:
        if "(pushed)" in ln:
            pushed += 1
            m = STORED_RE.search(ln)
            if m:
                stored = max(stored, int(m.group(1)))
            r = RATE_RE.search(ln)
            if r:
                rate = float(r.group(1))
        if "push failed" in ln:
            push_failed += 1
        if "put failed" in ln:
            put_failed += 1
        if "reconnecting" in ln:
            reconnects += 1
        if ln.startswith("source="):
            source = ln.strip()
        rr = RET_RE.search(ln)
        if rr:
            searches += 1
            last_retrieval_ms = float(rr.group(1))
        pr = PULL_RE.search(ln)
        if pr:
            last_pull_changed = pr.group(1)
    return {
        "stored": stored, "rate_per_s": rate, "pushed_batches": pushed,
        "push_failed": push_failed, "put_failed": put_failed, "reconnects": reconnects,
        "source": source,
        "searches": searches, "last_retrieval_ms": last_retrieval_ms,
        "last_pull_changed": last_pull_changed,
    }


def read_fleet():
    """Fleet node URLs: env FLEET (comma-sep) wins, else tools/fleet.txt (one URL per line,
    '#' comments). Always includes this agent's own origin first."""
    urls = []
    env = os.environ.get("FLEET", "")
    if env.strip():
        urls = [u.strip() for u in env.split(",") if u.strip()]
    else:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fleet.txt")
        try:
            with open(path) as f:
                for ln in f:
                    ln = ln.split("#")[0].strip()
                    if ln:
                        urls.append(ln)
        except FileNotFoundError:
            pass
    return urls


def fetch_telemetry(url):
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/telemetry", timeout=4) as r:
            d = json.loads(r.read().decode())
            d["__url"] = url
            d["__ok"] = True
            return d
    except Exception as e:  # noqa
        return {"__url": url, "__ok": False, "__err": str(e)}


def fleet_telemetry():
    urls = read_fleet()
    with ThreadPoolExecutor(max_workers=min(8, len(urls) or 1)) as ex:
        nodes = list(ex.map(fetch_telemetry, urls))
    return {"ts": time.time(), "count": len(nodes), "nodes": nodes}


def load_config():
    path = ARGS.config
    raw, parsed = "", {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        if tomllib:
            parsed = tomllib.loads(raw)
    except Exception as e:  # noqa
        raw = f"(could not read {path}: {e})"
    return {"path": path, "raw": raw, "parsed": parsed}


def telemetry():
    full = tail(ARGS.log, ARGS.lines)
    try:
        last_mtime = os.path.getmtime(ARGS.log)
    except OSError:
        last_mtime = None
    # "up" = log was written to in the last 30s (the node is actively doing work)
    up = last_mtime is not None and (time.time() - last_mtime) < 30
    return {
        "host": socket.gethostname(),
        "role": ARGS.role,
        "ts": time.time(),
        "agent_uptime_s": round(time.time() - START, 1),
        "log_path": ARGS.log,
        "log_mtime": last_mtime,
        "up": up,
        "config": load_config(),
        "metrics": parse_metrics(full),
        "recent_log": full,
        "errors": [ln for ln in full if ERR_RE.search(ln)][-40:],
    }


class H(BaseHTTPRequestHandler):
    def _send(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self._send({"error": "dashboard.html not found next to agent.py"}, 404)

    def do_GET(self):
        p = self.path.split("?")[0]
        if p == "/" or p == "/dashboard.html":
            self._send_html()
            return
        if p == "/health":
            self._send({"ok": True, "role": ARGS.role, "host": socket.gethostname()})
        elif p == "/telemetry":
            self._send(telemetry())
        elif p == "/config":
            self._send(load_config())
        elif p == "/logs":
            self._send({"lines": tail(ARGS.log, ARGS.lines)})
        elif p == "/fleet":
            self._send({"nodes": read_fleet()})
        elif p == "/fleet-telemetry":
            self._send(fleet_telemetry())
        else:
            self._send({"error": "not found", "endpoints": ["/health", "/telemetry", "/config", "/logs"]}, 404)

    def log_message(self, *a):  # silence default request logging
        pass


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", default=os.environ.get("AGENT_ROLE", "node"))
    ap.add_argument("--config", default=os.environ.get("AGENT_CONFIG", "configs/master.toml"))
    ap.add_argument("--log", default=os.environ.get("AGENT_LOG", ".work/logs/master.log"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("AGENT_PORT", "7071")))
    ap.add_argument("--bind", default=os.environ.get("AGENT_BIND", "0.0.0.0"))
    ap.add_argument("--lines", type=int, default=120)
    ARGS = ap.parse_args()
    srv = ThreadingHTTPServer((ARGS.bind, ARGS.port), H)
    print(f"cascade agent: role={ARGS.role} config={ARGS.config} log={ARGS.log} "
          f"serving http://{ARGS.bind}:{ARGS.port}/telemetry")
    srv.serve_forever()


if __name__ == "__main__":
    main()
