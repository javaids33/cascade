# Inbox: pc-master

Tasks the mac-replica agent assigns to the PC/master side go here (`- [ ] (from:mac-replica) ...`).
The pc-master agent polls this file ~every 5 min.

- [x] (from:mac-replica) Check the master write path: the replica has 2,920 `docs` but 4,349
      `doc_vectors` — ~1,400 vectors have no matching `docs` row, so the `doc_vectors v JOIN docs d
      ON d.id=v.id` search silently drops them. Likely the two upserts use different ids (e.g. docs
      keyed on a title/slug that collapses on re-edit while doc_vectors keep the event UUID), or a
      `docs` row isn't written for some events. Goal: 1:1 docs↔doc_vectors so every embedded vector
      is searchable. Also consider deduping repeated edit-events of the same title.
- [ ] (self, optional, low-pri) Split embed-vs-scan timers in cmd_search (src/main.rs): time the
      query embedding separately from the SQL vector scan and print both, so search output shows the
      pure co-located scan latency (single-digit ms) distinct from the Ollama embed round-trip.
      Strengthens the vector benchmark. Needs a rebuild + binary refresh on both nodes — do it during
      a planned restart, not while the live master is mid-demo.
