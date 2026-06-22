# TEST.md — network preflight + two-machine post-build check

How to verify, on the real two-machine setup, that the **master/slave configs are healthy, CDC
messages flow, and push-down distribution works**. Run the network preflight first; it's the usual
cause of "nothing works." Replace `192.168.1.6` with the PC's actual LAN IP everywhere.

## 0. Network preflight (from the Mac) — do this first

The PC runs the sync hub on `:8080` (started by the `.cmd` on the PC). Confirm the Mac can reach it:

```bash
curl http://192.168.1.6:8080/
```

**Expected: an HTTP `404` (not a timeout).** A 404 means the hub answered — it's up and on the LAN.
A **timeout** means it's unreachable: **re-run `start-master.cmd`** on the PC and **confirm both
machines are on the same `192.168.1.x` LAN** (same router/Wi-Fi). Check the PC's IP with `ipconfig`
and the Mac's with `ifconfig | grep "inet 192.168"`.

> On Windows the hub runs inside **WSL2**, which is NAT'd — binding `0.0.0.0:8080` there is **not**
> enough on its own. `start-master.cmd` adds a `netsh portproxy` rule (Windows :8080 → WSL :8080) +
> a firewall opening, and that's why **re-running it fixes the timeout** (WSL2's internal IP changes
> on reboot/restart, so the forward must be refreshed). On a native Linux/macOS master there's no
> NAT and binding `0.0.0.0` is sufficient.

## 1. Config contract (both machines, after `cargo build`)

The no-infra test that guards how the master/slave configs are parsed:

```bash
cargo test --test config_cases        # expect: ok (9 passed)
```

If this fails, `configs/*.toml` drifted from the code — fix before testing servers.

## 2. PC = master (producer): serve + CDC

One click: double-click **`start-master.cmd`** on the PC (it runs `start-master.sh` inside WSL —
builds if needed, checks Ollama, prints the LAN address, and serves). Or do it by hand:

```bash
./setup.sh                                     # or: cargo build --release
ollama pull all-minilm                         # embeds on the 3070
./start-master.sh                              # = cascade serve --config configs/master.toml
```

`start-master.sh` spawns the hub on `0.0.0.0:8080`, ingests, captures CDC, and pushes.

> **On a Windows PC**, `start-master.cmd` is the only thing you run — it installs the WSL build
> toolchain, bridges the WSL master to the Windows-host Ollama, sets up the WSL2→LAN portproxy, then
> calls `start-master.sh`. Ollama just needs to be running on Windows — no restart needed. Full
> details in [`docs/MASTER_SETUP.md`](docs/MASTER_SETUP.md).

In a second shell on the PC, confirm **CDC messages → OLAP** (the "running messages cdc" check):

```bash
cascade drain --config configs/master.toml     # prints "drained N changes ... (R rows)"
```

Healthy = `changes > 0` and DuckDB `rows > 0` — the CDC stream is captured and draining.

## 3. Mac = replica (consumer): pull + search (push-down distribution)

Edit `configs/replica.toml` → `sync.remote_url = "http://192.168.1.6:8080"`, then:

```bash
cascade search "what are the recent edits about science?" 5 --config configs/replica.toml
```

Healthy = it prints **Sources** + a **`[retrieval]`** time. Those documents were embedded on the PC
and arrived via push/pull — **push-down distribution works** (the Mac never embedded them). Add more
edges by copying `replica.toml` with a different `node.db`; each pulls the same vectors.

## 4. One-command gate (run on each machine)

```bash
./test.sh        # config contract + health + master role + replica role (local round-trip)
```

Green on **both** machines, **plus** the step-3 search returning the producer's docs, means:
master/slave healthy · CDC flowing · push-down distribution working.

## What "healthy" means (summary)

| Check | Healthy signal |
|---|---|
| Network | `curl <pc>:8080/` returns 404, not a timeout |
| Config contract | `cargo test --test config_cases` → 9 passed |
| Master server | `cascade serve` runs; hub answers |
| CDC messages | `cascade drain` reports changes + DuckDB rows |
| Push-down distribution | Mac `cascade search` returns docs the PC embedded |

## Optional: language clients

Non-Rust apps drive a node via the local gateway (`cascade gateway --config <cfg>`):
```bash
curl http://127.0.0.1:7070/health           # {ok, role}
curl "http://127.0.0.1:7070/search?q=cdc&k=3"
```
See [`clients/`](clients/) for Python / JS / Go wrappers.
