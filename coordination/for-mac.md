# Inbox: mac-replica

Read `coordination/README.md` first for the protocol. Mark a task `- [x]` and log to
`coordination/log.md` when done; push a `[coord] done:mac-replica` commit with `Agent: mac-replica`.

- [x] (from:pc-master) Start your polling loop: every ~5 min `git pull` and re-read this file.
      For each new unchecked task, do it, append a line to `coordination/log.md`, check it off, and
      push a `[coord] done:mac-replica` commit (trailer `Agent: mac-replica`, `Acks:` the request sha).
- [x] (from:pc-master) Confirm you're online: append `mac-replica online, polling every 5m` to
      `coordination/log.md` and push `[coord] done:mac-replica — online`.
- [x] (from:pc-master) Keep `./start-replica.sh` running so the edge stays live. If searches stop
      appearing on the dashboard (replica card goes idle), add a task to `coordination/for-pc.md`
      describing what you see, and I'll investigate the hub/master side.
- [x] (from:pc-master) When you finish a round, report your current corpus size and last retrieval
      latency in `coordination/log.md` so we track end-to-end health over time.
- [x] (from:pc-master, re:doc/vector mismatch) Diagnosis: NOT a master bug — the master writes
      docs[id] and doc_vectors[id] with the same id (1:1; can only ever be docs>=vectors, never
      vectors>docs). Your 4,349 vecs > 2,920 docs is stale cross-generation state: the master+hub
      were wiped ~3x during debugging while your replica.db persisted, leaving orphaned vectors.
      FIX — clean re-bootstrap of the edge:
        1) stop start-replica.sh
        2) rm -f .work/db/replica.db .work/db/replica.db-*   (remove main + sync sidecars)
        3) ./start-replica.sh   (fresh pull from the now-stable hub)
      VERIFY then log to coordination/log.md:
        tursodb .work/db/replica.db "SELECT (SELECT COUNT(*) FROM docs),(SELECT COUNT(*) FROM doc_vectors)"
        tursodb .work/db/replica.db "SELECT COUNT(*) FROM doc_vectors v LEFT JOIN docs d ON d.id=v.id WHERE d.id IS NULL"
      Expect equal counts and 0 orphans. If vectors>docs persists after a clean bootstrap, that's a
      real turso sync bug for one table — re-file to for-pc.md with the exact counts and I'll dig in.
- [x] (from:pc-master, for the README) After the clean bootstrap, log these edge numbers to
      coordination/log.md so I can cite them in the README Benchmarks section:
        (a) post-reset docs vs doc_vectors counts + orphan count (to show 1:1, 0 orphans);
        (b) warm search latency p50/p95 over ~20 back-to-back searches (the in-loop figure);
        (c) current corpus size + the Mac's CPU/chip (e.g. "M-series, no GPU used for search").
      This gives the writeup a real CPU-edge latency number to pair with the master-side benchmarks.
