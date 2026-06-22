"""Cascade client (Python, stdlib only). Talks to a local `cascade gateway`.

    from cascade_client import Cascade
    c = Cascade("http://127.0.0.1:7070")
    c.put("doc-1", "Turso has CDC + native replication", {"src": "demo"})   # master gateway
    for hit in c.search("what does turso do?", k=5):
        print(round(hit["score"], 3), hit["text"])

Run as a script for a quick smoke test:  python cascade_client.py
"""
import json
import urllib.parse
import urllib.request


class Cascade:
    def __init__(self, base="http://127.0.0.1:7070"):
        self.base = base.rstrip("/")

    def _get(self, path, params=None):
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.load(r)

    def _post(self, path, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            self.base + path, data=data,
            headers={"content-type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.load(r)

    def health(self):
        return self._get("/health")

    def put(self, id, text, meta=None):
        return self._post("/put", {"id": id, "text": text, "meta": meta or {}})

    def search(self, q, k=5):
        return self._get("/search", {"q": q, "k": k})["hits"]

    def drain(self):
        return self._post("/drain", {})


if __name__ == "__main__":
    c = Cascade()
    print("health:", c.health())
    c.put("py-1", "Cascade exposes Turso over a local HTTP gateway", {"src": "python"})
    for h in c.search("how do clients talk to a node?", k=3):
        print(f"  {h['score']:.3f}  {h['text']}")
