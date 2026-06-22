package main

import (
	"fmt"

	cascade "github.com/javaids33/turso-edge-olap/clients/go"
)

func main() {
	c := cascade.New("http://127.0.0.1:7070")
	h, err := c.Health()
	if err != nil {
		fmt.Println("gateway not reachable:", err)
		return
	}
	fmt.Println("health:", h)
	_ = c.Put("go-1", "Cascade exposes Turso over a local HTTP gateway", map[string]any{"src": "go"})
	hits, _ := c.Search("how do clients talk to a node?", 3)
	for _, hit := range hits {
		fmt.Printf("  %.3f  %s\n", hit.Score, hit.Text)
	}
}
