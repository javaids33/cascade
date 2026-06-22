# Running the master on a Windows PC (receiving from a Mac)

This PC is the **master / GPU producer**: it embeds incoming data on its GPU (via Ollama), captures
CDC, drains to DuckDB, and hosts the Turso **sync hub** that edge replicas (the Mac) pull from.

The single entry point is **`start-master.cmd`** — double-click it. Everything below is what it does
and why, plus how to verify the Mac actually receives.

## Why it isn't just "run the binary"

The Turso sync hub (`tursodb --sync-server`) is **Linux-only**, so on Windows the master runs inside
**WSL2 Ubuntu**. That creates three things a plain `cargo run` can't handle on Windows 10 — all
folded into `start-master.cmd`:

| # | Problem | What the .cmd does |
|---|---------|--------------------|
| 1 | Fresh WSL has no Rust toolchain | `start-master.sh` installs `rustup` once, then builds |
| 2 | The WSL master can't reach Ollama (it's on the Windows host's `127.0.0.1`, not WSL's localhost) | adds a portproxy `<host-gateway>:11434 → 127.0.0.1:11434` + firewall; `start-master.sh` points embedding there via `CASCADE_EMBED_URL`. No Ollama restart needed. |
| 3 | WSL2 is NAT'd — binding `0.0.0.0:8080` in WSL is **not** reachable from the Mac | `netsh portproxy` forwards Windows :8080 → WSL :8080 + firewall allow (the load-bearing fix) |

> Mirrored WSL networking would avoid the portproxy, but it needs **Windows 11**; this is Windows 10.

## Steps on this PC

1. **Double-click `start-master.cmd`.** It self-elevates (UAC prompt — needed for `netsh`/firewall),
   wires items 1–3 above, prints the **LAN address for the Mac**, then builds (first run only) and
   serves. Leave the window open; Ctrl-C stops the master.
2. **Ollama** just needs to be running on Windows (`curl http://localhost:11434/api/tags` → models).
   No restart or `OLLAMA_HOST` change — the `.cmd`'s portproxy reaches its `127.0.0.1` listener.
   `start-master.sh` auto-pulls `all-minilm` if missing.
3. **Re-run `start-master.cmd` after any reboot or WSL restart** — WSL2's internal IP changes, so both
   portproxy targets (the hub and Ollama) must be refreshed. (Re-running is also the fix if the Mac
   times out.)

## On the Mac (replica)

Edit `configs/replica.toml` → `sync.remote_url = "http://<PC-LAN-IP>:8080"` (the .cmd prints the IP),
then:

```bash
cargo build --release
cascade search "what are the recent edits about science?" 5 --config configs/replica.toml
```

It prints **Sources** + a `[retrieval]` time for documents the **PC embedded** — proof that
push-down distribution works (the Mac never embedded them).

## Verify the Mac can reach the hub

From the Mac:

```bash
curl http://<PC-LAN-IP>:8080/      # expect HTTP 404 (hub answered), NOT a timeout
```

- **404** → the portproxy + firewall are working; the Mac is on the hub.
- **timeout** → re-run `start-master.cmd` on the PC (refreshes the WSL-IP portproxy) and confirm both
  machines are on the same `192.168.1.x` LAN (`ipconfig` on the PC, `ifconfig | grep "inet 192.168"`
  on the Mac). A guest/AP-isolated network will also time out.

## Notes

- **Ollama runs on Windows** (native GPU). The WSL master reaches it at the WSL default-gateway IP
  on :11434; `start-master.sh` sets `CASCADE_EMBED_URL` to that automatically. On native Linux/macOS
  there's no bridge — `localhost:11434` is used as-is.
- **`CASCADE_EMBED_URL`** overrides `[embedding].url` from the config without editing the committed
  `configs/*.toml`. It also drives the generation endpoint.
- **One-way sync** (master→replica). No multi-primary / conflict resolution.
