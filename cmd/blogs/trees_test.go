package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/testchain"
)

var treeLine = regexp.MustCompile(`interval (\d+): funding tree ([0-9a-f]{64}) from (\S.*)`)

// pruned is a node that no longer serves anything by txid, as one that
// prunes fully spent transactions answers for a spent funding tree.
func pruned(t *testing.T) string {
	t.Helper()
	s := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(s.Close)
	return s.URL
}

func TestHostsServeTheFeeTreesAPrunedNodeNoLongerHas(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "2")
	h.produce(linesOf(10, 2), stream, "-max-lines", "1")
	node := h.env["BLOGS_ASSET"]

	// The node has pruned the fee tree: the host answers it, with its proof.
	h.env["BLOGS_ASSET"] = pruned(t)
	r := h.must("", "verify-interval", stream, producer, "2", "-v")
	m := treeLine.FindStringSubmatch(r.stderr)
	if !strings.Contains(r.stdout, "VERIFIED interval 2") || m == nil || m[3] != h.hostS.URL {
		t.Fatalf("verify with a pruned node:\n%s\n%s", r.stdout, r.stderr)
	}
	tree := m[2]

	// A host that predates the tree class, and the node pruned: incomplete,
	// naming each thing asked.
	h.host.SetNoTrees(true)
	r = h.run("", "verify-interval", stream, producer, "2")
	h.want(r, exitIncomplete, "INCOMPLETE interval 2: interval 1: input 0 spends "+tree)
	for _, has := range []string{"a funding tree nothing asked serves", h.hostS.URL + " did not answer the tree class", "the node " + h.env["BLOGS_ASSET"] + " did not answer"} {
		if !strings.Contains(r.stdout, has) {
			t.Fatalf("no %q in:\n%s", has, r.stdout)
		}
	}
	// With no copy of the chain configured the reason says so.
	h.env["BLOGS_ASSET"] = ""
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitIncomplete, "no copy of the chain is configured")
	// Such a host and a node that still has the tree: the node answers.
	h.env["BLOGS_ASSET"] = node
	r = h.must("", "verify-interval", stream, producer, "2", "-v")
	if m := treeLine.FindStringSubmatch(r.stderr); m == nil || m[2] != tree || m[3] != "the node "+node {
		t.Fatalf("verify from the node:\n%s\n%s", r.stdout, r.stderr)
	}
	h.host.SetNoTrees(false)

	// A host that answers the tree question with another mined transaction
	// (here, interval 1's token) is refused, and the audit is incomplete
	// rather than misled.
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		var q struct {
			Service string         `json:"service"`
			Query   map[string]any `json:"query"`
		}
		if json.Unmarshal(body, &q) == nil && q.Query["tree"] != nil {
			q.Query = map[string]any{"stream": stream, "producer": producer, "interval": 1}
			body, _ = json.Marshal(q)
		}
		fwd, _ := http.NewRequestWithContext(req.Context(), req.Method, h.hostS.URL+req.URL.Path, bytes.NewReader(body))
		fwd.Header = req.Header.Clone()
		res, err := http.DefaultClient.Do(fwd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer res.Body.Close()
		w.WriteHeader(res.StatusCode)
		_, _ = io.Copy(w, res.Body)
	}))
	t.Cleanup(liar.Close)
	h.env["BLOGS_HOSTS"], h.env["BLOGS_ASSET"] = liar.URL, pruned(t)
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitIncomplete, "answered something refused: REFUSED-DECODE asked for transaction "+tree)
	// The honest host beside it still serves the audit.
	h.env["BLOGS_HOSTS"] = liar.URL + "," + h.hostS.URL
	h.must("", "verify-interval", stream, producer, "2")

	// Backfill carries the tree from the sibling, not the node.
	sibling := httptest.NewServer(testchain.NewHost(h.chain))
	t.Cleanup(sibling.Close)
	r = h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sibling.URL, "-interval", "1")
	if !strings.Contains(r.stdout, "interval 1: funding tree "+tree+" submitted from "+h.hostS.URL) || !strings.Contains(r.stdout, "interval 1: filled from the sibling") {
		t.Fatalf("backfill with a pruned node:\n%s\n%s", r.stdout, r.stderr)
	}
	h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sibling.URL, "-interval", "2")
	h.env["BLOGS_HOSTS"] = sibling.URL
	r = h.must("", "verify-interval", stream, producer, "2", "-v")
	if m := treeLine.FindStringSubmatch(r.stderr); m == nil || m[3] != sibling.URL {
		t.Fatalf("the filled sibling does not serve the tree:\n%s\n%s", r.stdout, r.stderr)
	}
}

func TestArchiveKeepsFeeTreesAndIsAnAuditSource(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "2")
	h.produce(linesOf(10, 2), stream, "-max-lines", "1")
	h.env["BLOGS_ASSET"] = pruned(t)
	dir := t.TempDir()
	r := h.must("", "archive", stream, producer, "-dir", dir)
	if !strings.Contains(r.stdout, "kept 1 funding tree(s) with their proofs in trees/") || !strings.Contains(r.stdout, "intervals 1 to 2 proven") {
		t.Fatalf("archive:\n%s\n%s", r.stdout, r.stderr)
	}
	root := filepath.Join(dir, stream, producer)
	trees, _ := filepath.Glob(filepath.Join(root, "trees", "*.beef"))
	if len(trees) != 1 {
		t.Fatalf("trees: %v", trees)
	}
	online := h.must("", "verify-interval", stream, producer, "2")

	// The archive alone, with no host and a pruned node, serves the audit.
	hosts := h.env["BLOGS_HOSTS"]
	h.env["BLOGS_HOSTS"] = ""
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitUsage, "no overlay host configured")
	r = h.must("", "verify-interval", stream, producer, "2", "-archive", dir, "-v")
	if r.stdout != online.stdout || !strings.Contains(r.stderr, "from the archive "+root) {
		t.Fatalf("audit from the archive:\n%s\n%s\nwant:\n%s", r.stdout, r.stderr, online.stdout)
	}
	h.want(h.run("", "verify-interval", stream, producer, "2", "-archive", t.TempDir()), exitUsage, "holds no archive of this stream")

	// A tree file that holds another transaction is refused, not believed.
	anchors, _ := filepath.Glob(filepath.Join(root, "anchors", "0000000000000001-*.beef"))
	kept, _ := os.ReadFile(trees[0])
	other, _ := os.ReadFile(anchors[0])
	if err := os.WriteFile(trees[0], other, 0o600); err != nil {
		t.Fatal(err)
	}
	h.want(h.run("", "verify-interval", stream, producer, "2", "-archive", dir), exitIncomplete, "answered something refused: REFUSED-DECODE asked for transaction")
	// With a host beside the archive, the host's copy serves.
	h.env["BLOGS_HOSTS"] = hosts
	h.must("", "verify-interval", stream, producer, "2", "-archive", dir)

	// An archive begun before trees were kept catches up on its next pass.
	if err := os.RemoveAll(filepath.Join(root, "trees")); err != nil {
		t.Fatal(err)
	}
	var cur map[string]any
	raw, _ := os.ReadFile(filepath.Join(root, "archive.json"))
	if err := json.Unmarshal(raw, &cur); err != nil || cur["trees"] != float64(2) {
		t.Fatalf("cursor: %s", raw)
	}
	delete(cur, "trees")
	raw, _ = json.Marshal(cur)
	if err := os.WriteFile(filepath.Join(root, "archive.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// A host that does not serve trees, and a pruned node: the pass is
	// done, and says the tree is not kept yet.
	h.host.SetNoTrees(true)
	r = h.must("", "archive", stream, producer, "-dir", dir)
	if strings.Contains(r.stdout, "funding tree(s)") || !strings.Contains(r.stderr, "a funding tree is not kept yet: interval 1: funding tree") {
		t.Fatalf("pass without a tree source:\n%s\n%s", r.stdout, r.stderr)
	}
	h.host.SetNoTrees(false)
	r = h.must("", "archive", stream, producer, "-dir", dir)
	back, _ := os.ReadFile(trees[0])
	if !strings.Contains(r.stdout, "kept 1 funding tree(s)") || !bytes.Equal(back, kept) {
		t.Fatalf("catch-up pass:\n%s\n%s", r.stdout, r.stderr)
	}
	r = h.must("", "archive", stream, producer, "-dir", dir)
	if strings.Contains(r.stdout, "funding tree(s)") {
		t.Fatalf("a pass with nothing new kept a tree:\n%s", r.stdout)
	}
}
