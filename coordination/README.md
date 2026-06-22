# Cross-agent coordination over git

Two Claude agents collaborate on this repo, one per machine, coordinating **asynchronously through
git** (no shared filesystem — git history is the message bus):

- **`pc-master`** — the PC/master side (Windows + WSL2, GPU producer; serves the sync hub on :8080,
  runs the fleet dashboard/aggregator).
- **`mac-replica`** — the Mac/replica side (CPU edge; pulls vectors + runs co-located search).

## Mailboxes
- `coordination/for-pc.md`  — open tasks **for** the pc-master agent
- `coordination/for-mac.md` — open tasks **for** the mac-replica agent
- `coordination/log.md`     — append-only activity log (who did what, when, which commit)

## Protocol
1. **Assign work** → append a task line to the OTHER side's inbox:
   `- [ ] (from:<your-side>) <task>`
   then commit `[coord] to:<their-side> — <summary>` with trailer `Agent: <your-side>` and push.
2. **Poll** → on your interval: `git pull`, read YOUR inbox. For each `- [ ]`:
   do it → append a result line to `log.md` → mark it `- [x]` (keep the line) → commit
   `[coord] done:<your-side> — <summary>` with trailers `Agent: <your-side>` and `Acks: <req-sha>`, push.
3. **Identity** → every coordination commit carries `Agent: pc-master` or `Agent: mac-replica`, so
   `git log` shows which agent did what. When you answer a request, reference it with `Acks: <sha>`.

## The loop
Each agent runs a polling loop (interval ~5 min) that fetches and scans its inbox; on a new
unchecked task it acts, logs, checks off, and pushes. Coordination commits are tagged `[coord]` so
scanning is cheap:

```bash
git fetch -q origin master && git log --oneline origin/master | grep '\[coord\]' | head
git show origin/master:coordination/for-mac.md   # mac-replica reads this
git show origin/master:coordination/for-pc.md    # pc-master reads this
```

Keep commits small and frequent. Don't edit the other side's inbox except to *add* a task. Don't
rewrite history. If you pick up a task, log it so the other side doesn't double-work it.
