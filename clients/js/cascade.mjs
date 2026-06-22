// Cascade client (JavaScript/TypeScript, zero deps — uses global fetch, Node >= 18 / Deno / Bun).
// Talks to a local `cascade gateway`.
//
//   import { Cascade } from "./cascade.mjs";
//   const c = new Cascade("http://127.0.0.1:7070");
//   await c.put("doc-1", "Turso has CDC + native replication", { src: "demo" });  // master gateway
//   for (const hit of await c.search("what does turso do?", 5))
//     console.log(hit.score.toFixed(3), hit.text);
//
// Run directly for a quick smoke test:  node cascade.mjs

export class Cascade {
  constructor(base = "http://127.0.0.1:7070") {
    this.base = base.replace(/\/$/, "");
  }
  async health() {
    return (await fetch(this.base + "/health")).json();
  }
  async put(id, text, meta = {}) {
    const r = await fetch(this.base + "/put", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id, text, meta }),
    });
    return r.json();
  }
  async search(q, k = 5) {
    const u = new URL(this.base + "/search");
    u.searchParams.set("q", q);
    u.searchParams.set("k", String(k));
    const r = await fetch(u);
    return (await r.json()).hits;
  }
  async drain() {
    return (await fetch(this.base + "/drain", { method: "POST" })).json();
  }
}

// Smoke test when run directly.
if (import.meta.url === `file://${process.argv[1]}`) {
  const c = new Cascade();
  console.log("health:", await c.health());
  await c.put("js-1", "Cascade exposes Turso over a local HTTP gateway", { src: "js" });
  for (const h of await c.search("how do clients talk to a node?", 3)) {
    console.log(`  ${h.score.toFixed(3)}  ${h.text}`);
  }
}
