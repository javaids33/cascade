#!/usr/bin/env python3
"""Cascade fleet MCP server — zero-dependency stdio MCP server that lets an assistant query the
fleet's telemetry/logs as native tools. It just talks HTTP to the agents (default master at
http://localhost:7071) and the agent's /fleet-telemetry aggregator.

Register in .mcp.json:
  {"mcpServers": {"cascade-fleet": {"command": "python", "args": ["tools/mcp_server.py"]}}}
Activates on the next Claude Code start. Tools:
  - fleet_status(master_url?)        compact health of every node
  - node_telemetry(url)             full telemetry JSON for one node
  - node_logs(url, lines?)          recent log lines for one node

Protocol: JSON-RPC 2.0, newline-delimited over stdio. stdout = protocol only; logs go to stderr.
"""
import json, sys, urllib.request

DEFAULT_MASTER = "http://localhost:7071"
PROTOCOL = "2024-11-05"

TOOLS = [
    {
        "name": "fleet_status",
        "description": "Compact health of the whole Cascade fleet (every node's role, up/idle, key "
                       "metrics, last error, last log line) via a master agent's /fleet-telemetry.",
        "inputSchema": {"type": "object", "properties": {
            "master_url": {"type": "string", "description": f"Master agent base URL (default {DEFAULT_MASTER})"}}},
    },
    {
        "name": "node_telemetry",
        "description": "Full telemetry JSON for a single Cascade node agent (config, metrics, recent log, errors).",
        "inputSchema": {"type": "object", "properties": {
            "url": {"type": "string", "description": "Node agent base URL, e.g. http://192.168.1.17:7071"}},
            "required": ["url"]},
    },
    {
        "name": "node_logs",
        "description": "Recent raw log lines from a single Cascade node agent.",
        "inputSchema": {"type": "object", "properties": {
            "url": {"type": "string"}, "lines": {"type": "integer", "description": "how many lines (default 80)"}},
            "required": ["url"]},
    },
]


def http_json(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def fmt_fleet(master_url):
    d = http_json(master_url.rstrip("/") + "/fleet-telemetry")
    nodes = d.get("nodes", [])
    out = [f"fleet: {len(nodes)} node(s) via {master_url}"]
    for n in nodes:
        if not n.get("__ok"):
            out.append(f"  x OFFLINE {n.get('__url')} -- {n.get('__err','')}"); continue
        m = n.get("metrics", {}); role = n.get("role", "?")
        out.append(f"  [{'UP' if n.get('up') else 'idle'}] {role} {n.get('host','?')} {n.get('__url')}")
        if role == "replica":
            out.append(f"      searches={m.get('searches',0)} last_retrieval_ms={m.get('last_retrieval_ms')} last_pull={m.get('last_pull_changed')}")
        else:
            out.append(f"      stored={m.get('stored',0)} pushed={m.get('pushed_batches',0)} push_failed={m.get('push_failed',0)} reconnects={m.get('reconnects',0)}")
        errs = n.get("errors", [])
        if errs:
            out.append(f"      last error: {errs[-1][:160]}")
    return "\n".join(out)


def call_tool(name, args):
    if name == "fleet_status":
        return fmt_fleet(args.get("master_url") or DEFAULT_MASTER)
    if name == "node_telemetry":
        d = http_json(args["url"].rstrip("/") + "/telemetry")
        d.pop("recent_log", None)  # trim; use node_logs for the log
        return json.dumps(d, indent=2)[:6000]
    if name == "node_logs":
        n = int(args.get("lines", 80))
        d = http_json(args["url"].rstrip("/") + f"/logs?n={n}")
        return "\n".join(d.get("lines", [])[-n:])
    raise ValueError(f"unknown tool {name}")


def send(msg):
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception:
            continue
        mid = req.get("id")
        method = req.get("method", "")
        if method == "initialize":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": PROTOCOL,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "cascade-fleet", "version": "0.1.0"}}})
        elif method == "notifications/initialized":
            pass  # notification, no reply
        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            params = req.get("params", {})
            try:
                text = call_tool(params.get("name"), params.get("arguments") or {})
                send({"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": text}]}})
            except Exception as e:  # noqa
                send({"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": f"error: {e}"}], "isError": True}})
        elif method == "ping":
            send({"jsonrpc": "2.0", "id": mid, "result": {}})
        elif mid is not None:
            send({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"method not found: {method}"}})


if __name__ == "__main__":
    main()
