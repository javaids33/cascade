// Package cascade is a tiny Go client for a local `cascade gateway` (stdlib only).
//
//	c := cascade.New("http://127.0.0.1:7070")
//	_ = c.Put("doc-1", "Turso has CDC + native replication", map[string]any{"src": "demo"})
//	hits, _ := c.Search("what does turso do?", 5)
//	for _, h := range hits { fmt.Printf("%.3f %s\n", h.Score, h.Text) }
package cascade

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Base string
	HTTP *http.Client
}

func New(base string) *Client {
	return &Client{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 120 * time.Second}}
}

type Hit struct {
	ID    string          `json:"id"`
	Text  string          `json:"text"`
	Meta  json.RawMessage `json:"meta"`
	Score float64         `json:"score"`
}

func (c *Client) Health() (map[string]any, error) {
	var out map[string]any
	return out, c.do("GET", "/health", nil, &out)
}

func (c *Client) Put(id, text string, meta map[string]any) error {
	if meta == nil {
		meta = map[string]any{}
	}
	return c.do("POST", "/put", map[string]any{"id": id, "text": text, "meta": meta}, nil)
}

func (c *Client) Search(q string, k int) ([]Hit, error) {
	var out struct {
		Hits []Hit `json:"hits"`
	}
	path := "/search?" + url.Values{"q": {q}, "k": {strconv.Itoa(k)}}.Encode()
	return out.Hits, c.do("GET", path, nil, &out)
}

func (c *Client) Drain() (map[string]any, error) {
	var out map[string]any
	return out, c.do("POST", "/drain", nil, &out)
}

func (c *Client) do(method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.Base+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("cascade %s %s: %d %s", method, path, resp.StatusCode, string(data))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
