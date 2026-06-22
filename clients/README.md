# Cascade clients (any language)

Non-Rust apps talk to a node through the **local HTTP gateway** — run it co-located with the node so
the app still gets local, co-located vector search:

```bash
cascade gateway --config configs/replica.toml          # edge: search (default 127.0.0.1:7070)
cascade gateway --config configs/master.toml --bind 0.0.0.0:7070   # master: also put / drain
```

## HTTP API

| Method | Path | Body / Query | Role | Returns |
|---|---|---|---|---|
| GET  | `/health` | — | any | `{ok, role}` |
| POST | `/put` | `{id, text, meta?}` | master | `{ok, id}` (embeds + stores + push) |
| GET  | `/search` | `?q=...&k=5` | any | `{hits: [{id, text, meta, score}]}` |
| POST | `/drain` | — | master | OLAP stats (CDC → DuckDB) |

`score` is cosine distance (lower = closer). The gateway embeds via the node's configured Ollama
model, so clients never need an embedding model themselves.

## Clients

- **Python** — [`python/cascade_client.py`](python/cascade_client.py) (stdlib only):
  `python clients/python/cascade_client.py`
- **JavaScript/TypeScript** — [`js/cascade.mjs`](js/cascade.mjs) (zero deps, global `fetch`,
  Node ≥18 / Deno / Bun): `node clients/js/cascade.mjs`
- **Go** — [`go/`](go/) (stdlib only): `cd clients/go && go run ./cmd/demo`

Each is ~40 lines exposing `health / put / search / drain`. Adding another language is just an HTTP
wrapper over the table above.
