# Lab: "Living Knowledge Base" — live firehose → edge RAG on Turso

A two-machine lab that exercises **every** Turso superpower at once: a **PC master** ingests a live
firehose, embeds each item on the 3070 (Ollama), and writes to Turso with **CDC** on; that stream
feeds a **DuckDB OLAP** lane *and* replicates over **native sync** to a **Mac edge**, which answers
questions with **co-located vector search** (`vector_distance_cos`) + a local LLM — **no network
round-trip to a vector DB**.

```
  PC  (master · WSL2/Linux · RTX 3070 · Ollama)            Mac (edge · Ollama-Metal)
  ┌──────────────────────────────────────────┐  native    ┌────────────────────────────┐
  │ cascade serve (master.toml)                   │   sync     │ cascade search (replica.toml)  │
  │   Wikimedia firehose → embed (GPU)        │  :8080     │   pull() latest            │
  │   → Turso docs + doc_vectors (CDC on) ────┼───────────▶│   embed query (local)      │
  │ cascade drain (master.toml)                   │  hub db    │   vector_distance_cos (1ms)│
  │   docs CDC → DuckDB (OLAP lane)           │            │   → local LLM → cited answer│
  └──────────────────────────────────────────┘            └────────────────────────────┘
```

**Why this beats the conventional stack:** Turso is OLTP + CDC + replication + vector store in one
embedded file. The equivalent otherwise is Postgres + pgvector + Debezium + Kafka + Pinecone +
Litestream. Here it's two small processes and a sync port.

> Honest caveat: Turso 0.6.1 vector search is **brute-force cosine (no ANN index)**. At an edge
> corpus of ~10k–100k chunks it's still single-digit ms; the win we demonstrate is **co-location +
> edge locality + live sync**, not beating a GPU ANN index. Keep the edge corpus in that range.

---

## 0. One rule that must hold on both machines

Documents are embedded on the **PC**; queries are embedded on the **Mac**. They must use the **same
embedding model** so the vectors share a space:

```
EMBED_MODEL=all-minilm   EMBED_DIM=384      # F32_BLOB(384) — set identically on both
```

---

## 1. Networking (PC ↔ Mac)

The code only needs `TURSO_REMOTE_URL=http://<PC-ip>:8080`. Transport is your choice — pick one:

- **Tailscale (recommended, easiest):** install on both, `sudo tailscale up`, then
  `tailscale ip -4` on the PC gives a `100.x.y.z` address that works across any network/NAT. Use it
  as `<PC-ip>`. Open-source client; leave anytime. Swap later for LAN IP / WireGuard / Netbird /
  Headscale with **zero code change** — it's just the env var.
- **Same LAN:** use the PC's LAN IP (`ipconfig`); open TCP 8080 on the PC firewall.

The Mac dials `<PC-ip>:8080`. On a **Windows PC the hub runs in WSL2 (NAT'd)**, so binding
`0.0.0.0:8080` is not enough by itself — you also need a `netsh portproxy` forward (Windows :8080 →
WSL :8080) plus the firewall opening. `start-master.cmd` sets all of this up automatically; see
[`docs/MASTER_SETUP.md`](docs/MASTER_SETUP.md). On a native Linux/macOS master, binding `0.0.0.0` is
sufficient.

---

## 2. Setup — PC (master)

```bash
# prerequisites: Rust ≥1.92, git, Ollama (with the 3070), Tailscale
git pull
./setup.sh                          # downloads tursodb CLI + cargo build --release
ollama pull all-minilm              # 384-d embedder, runs on the 3070

# configs/master.toml already has source="wikimedia", serve=true, olap.duckdb set.
# A) serve = spawn the sync hub (bind 0.0.0.0) + embed the firehose + push. Runs continuously.
./target/release/cascade serve --config configs/master.toml

# B) any time (separate shell): drain the docs CDC stream → DuckDB (doesn't touch the live table)
./target/release/cascade drain --config configs/master.toml
```

Tuning is in `configs/master.toml`: `[source] wiki` (`enwiki`, or `""` = all wikis for a throughput
stress test), `namespace` (`0` = articles), `kind` (`wikimedia|hn|demo`), `[sync] push_every`.

## 3. Setup — Mac (edge)

```bash
# prerequisites: Rust ≥1.92, git, Ollama, Tailscale
git pull
./setup.sh
ollama pull all-minilm              # same embedder as the PC (required)
ollama pull qwen2.5:1.5b            # a generator for answers (or any chat model)

# Edit configs/replica.toml: set sync.remote_url = http://<PC-ip>:8080
# Ask questions against the edge replica — pulls latest from the PC, then local vector search.
./target/release/cascade search "what are the recent edits about science?" 5 --config configs/replica.toml
```

The `search` output prints the **`[retrieval]`** latency (local, ~1–3 ms) — the edge win — plus the
cited sources and (if `[generation] model` is set) an answer.

---

## 3b. AI distribution: one GPU producer → many edges (Pattern P4)

The PC is the **producer**: it embeds the corpus once on the 3070 and `push()`es finished vectors.
Turso sync is **one-primary → many-replicas**, so you add edges by copying `configs/replica.toml`
with a distinct `node.db` — each is a full local replica that does cosine search on **CPU, no GPU, no
embed model for documents**. You distribute *results*, not *compute*.

```bash
# more edges (any machine on the tailnet/LAN), each its own replica config (distinct node.db):
cascade search "..." --config configs/edge_A.toml
cascade search "..." --config configs/edge_B.toml
```

Only the *query* is embedded on the edge (a 384-d `all-minilm` is fine on CPU). If an edge has no
model at all, embed the query on the producer and ship just the vector. The expensive part — embedding
the whole stream — happens once, where the GPU is.

---

## 4. What each piece measures

| Capability | Command | The number that matters |
|---|---|---|
| Co-located vector search | `cascade search --config replica.toml` | `[retrieval]` ms — local, no network |
| Live propagation | `serve` on PC, `search` on Mac | result freshness after `pull` |
| CDC overhead | `cascade cdc-overhead` (synthetic) | % throughput cost, ~2× storage |
| CDC → OLAP | `cascade drain --config master.toml` | changes drained, docs in DuckDB |
| Native replication | `cascade run-all` (synthetic) | sub-second replica bootstrap |
| Edge vs central | `search` on Mac vs forced to PC | latency delta = the locality win |

---

## 5. Docker (optional)

Ollama stays on the **host** of each machine (GPU/Metal), so containers point at it via
`host.docker.internal`. See `docker/` — `Dockerfile` builds `cascade` + bundles the `tursodb` CLI;
`docker-compose.master.yml` runs the hub + ingest + periodic olap on the PC;
`docker-compose.edge.yml` runs an interactive edge for `rag`. Native (above) is the faster dev loop
and matches "PC can change master-side code easily"; Docker is for keeping the master always-on.

---

## 6. Repo workflow

Develop on the Mac, `git push`; on the PC `git pull` and `cargo build --release`. The lab is all in
this repo (`src/ingest.rs`, `src/rag.rs`, `src/lab_olap.rs`, `src/ollama.rs`, `src/labdb.rs`,
`docker/`). Results land in `results/lab_olap.json`.
