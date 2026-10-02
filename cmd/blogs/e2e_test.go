//go:build e2e

package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lightwebinc/blogs/internal/produce"
)

// The client end to end: the blogs command against two local reference
// overlay hosts running the blogs host module, over a local chain (package
// testchain: the node's RPC and asset API, and the header source both hosts
// check proofs against). Nothing leaves the machine.
//
//	make e2e-client REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
//
// It needs Docker for the hosts' MySQL.

const e2ePassword = "blogs-e2e"

type refHost struct {
	t    *testing.T
	dir  string
	node string
	db   int
	name string
	port int
	env  []string
	proc *exec.Cmd
	out  *bytes.Buffer
	url  string
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func mysql(t *testing.T) int {
	t.Helper()
	id := docker(t, "run", "-d", "--rm", "-e", "MYSQL_ROOT_PASSWORD="+e2ePassword, "-e", "MYSQL_DATABASE=overlay_a",
		"-p", "127.0.0.1::3306", "--tmpfs", "/var/lib/mysql", "mysql:9")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	port := docker(t, "port", id, "3306/tcp")
	port = port[strings.LastIndex(strings.Split(port, "\n")[0], ":")+1:]
	p, _ := strconv.Atoi(strings.Split(port, "\n")[0])
	for i := 0; i < 120; i++ {
		if exec.Command("docker", "exec", id, "mysql", "-h127.0.0.1", "-uroot", "-p"+e2ePassword, "-e", "create database if not exists overlay_b").Run() == nil {
			return p
		}
		time.Sleep(time.Second)
	}
	t.Fatal("MySQL did not come up")
	return 0
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (h *refHost) start() {
	h.t.Helper()
	h.out = &bytes.Buffer{}
	h.proc = exec.Command(h.node, filepath.Join(h.dir, "dist", "index.js"))
	h.proc.Env = h.env
	h.proc.Stdout, h.proc.Stderr = h.out, h.out
	if err := h.proc.Start(); err != nil {
		h.t.Fatal(err)
	}
	for i := 0; i < 120; i++ {
		if resp, err := http.Get(h.url + "/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.stop()
	h.t.Fatalf("host %s did not become ready:\n%s", h.name, h.out)
}

func (h *refHost) stop() {
	if h.proc == nil || h.proc.Process == nil {
		return
	}
	_ = h.proc.Process.Signal(syscall.SIGTERM)
	_ = h.proc.Wait()
	h.proc = nil
}

// refHosts stages the bundle as a deployment places it (in a modules/
// directory whose only node_modules is the reference host's), starts one
// MySQL and a reference host per name, each admitting only this producer on
// the stream, and returns them by name. It skips the test when no
// reference host is configured.
func refHosts(t *testing.T, chainURL, stream, producer string, names ...string) map[string]*refHost {
	t.Helper()
	dir := os.Getenv("BLOGS_E2E_REFERENCE_HOST")
	node := os.Getenv("BLOGS_E2E_NODE")
	if node == "" {
		node = "node"
	}
	bundle, err := filepath.Abs("../../host/bundle/blogs-module.js")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatalf("the host module bundle is not built: %v", err)
	}
	staging := t.TempDir()
	if err := os.Symlink(filepath.Join(dir, "node_modules"), filepath.Join(staging, "node_modules")); err != nil {
		t.Fatal(err)
	}
	module := filepath.Join(staging, "modules", "blogs", "blogs-module.js")
	if err := os.MkdirAll(filepath.Dir(module), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(module, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	db := mysql(t)
	hosts := map[string]*refHost{}
	for _, name := range names {
		port := freePort(t)
		rh := &refHost{t: t, dir: dir, node: node, name: name, port: port, db: db, url: fmt.Sprintf("http://127.0.0.1:%d", port)}
		rh.env = []string{
			"PATH=" + os.Getenv("PATH"),
			"OVERLAY_TOPICS=tm_log_" + stream,
			"OVERLAY_MODULES=" + module,
			"BLOGS_STREAMS=" + stream + "=" + producer,
			fmt.Sprintf("OVERLAY_KNEX_URL=mysql://root:%s@127.0.0.1:%d/overlay_%s", e2ePassword, db, name),
			"OVERLAY_CHAIN_TRACKER_URL=" + chainURL,
			"OVERLAY_ADMIN_TOKEN=blogs-e2e",
			"OVERLAY_LISTEN=127.0.0.1",
			"OVERLAY_PORT=" + strconv.Itoa(port),
		}
		rh.start()
		t.Cleanup(rh.stop)
		hosts[name] = rh
	}
	return hosts
}

// e2eSetup skips without a reference host, and otherwise shortens the
// publish retries for the test.
func e2eSetup(t *testing.T) {
	t.Helper()
	if os.Getenv("BLOGS_E2E_REFERENCE_HOST") == "" {
		t.Skip("set BLOGS_E2E_REFERENCE_HOST (make e2e-client REFERENCE_HOST=...)")
	}
	old := produce.Retries
	produce.Retries = []time.Duration{50 * time.Millisecond}
	t.Cleanup(func() { produce.Retries = old })
}

func TestClientE2E(t *testing.T) {
	e2eSetup(t)
	// The harness's chain is the one both hosts check proofs against; its
	// stand-in host is not used.
	h := newHarness(t)
	chainS := h.chainS
	stream, producer := h.producer()
	step := func(what string) { t.Logf("ok  %s", what) }
	step("init, fund and stream new " + stream)

	hosts := refHosts(t, chainS.URL, stream, producer, "a", "b")
	a, b := hosts["a"], hosts["b"]
	h.env["BLOGS_FACADE"], h.env["BLOGS_HOSTS"] = a.url, a.url
	if !strings.Contains(a.out.String(), "blogs module mounted") || !strings.Contains(a.out.String(), "@lightwebinc/bcommon") {
		t.Fatalf("host a did not log the bundle's mount line:\n%s", a.out)
	}
	step("two reference hosts loading the bundle, admitting only this producer")

	// Interval 1: full-bytes lines, a hostile line among them.
	in1 := linesOf(0, 20) + "colour \x1b[31mred\x1b[0m title\x1b]0;owned\x07 end\n"
	r := h.produce(in1, stream, "-max-lines", "5", "-poll", "50ms")
	if !strings.Contains(r.stdout, "published seq 1 to 5") || !strings.Contains(r.stdout, "anchored to seq 5: interval 1") {
		t.Fatalf("produce 1:\n%s\n%s", r.stdout, r.stderr)
	}
	if strings.Contains(r.stderr, "admitted nothing") {
		t.Fatalf("a publish was not admitted:\n%s", r.stderr)
	}
	step("produce: 21 lines in 5 full-bytes batches, anchored as interval 1 after it mined")
	r = h.must("", "read", stream, producer)
	if !strings.HasPrefix(r.stdout, linesOf(0, 20)) || !strings.HasSuffix(r.stdout, "colour red title end\n") || strings.ContainsAny(r.stdout, "\x1b\x07") {
		t.Fatalf("read 1: %q", r.stdout)
	}
	step("read: every batch verified and linked, the hostile line filtered")
	r = h.must("", "verify-interval", stream, producer, "1")
	if !strings.Contains(r.stdout, "VERIFIED interval 1") {
		t.Fatalf("verify 1:\n%s", r.stdout)
	}
	step("verify-interval 1: VERIFIED against the reference host")

	// Interval 2: a second run, gzip; interval 3: digest-only.
	r = h.produce(linesOf(100, 12), stream, "-max-lines", "4", "-encoding", "gzip", "-poll", "50ms")
	if !strings.Contains(r.stdout, "published seq 6 to 8") || !strings.Contains(r.stdout, "interval 2") {
		t.Fatalf("produce 2:\n%s\n%s", r.stdout, r.stderr)
	}
	content := t.TempDir()
	r = h.produce(linesOf(200, 6), stream, "-max-lines", "3", "-profile", "digest", "-content-dir", content,
		"-url", "https://logs.example.com/c/", "-poll", "50ms")
	if !strings.Contains(r.stdout, "published seq 9 to 10") || !strings.Contains(r.stdout, "interval 3") {
		t.Fatalf("produce 3:\n%s\n%s", r.stdout, r.stderr)
	}
	if strings.Contains(r.stderr, "admitted nothing") {
		t.Fatalf("a publish was not admitted:\n%s", r.stderr)
	}
	step("produce again: the stream continues at seq 6 (gzip, interval 2) and seq 9 (digest-only, interval 3)")
	r = h.must("", "read", stream, producer, "-from", "6", "-content-dir", content)
	if r.stdout != linesOf(100, 12)+linesOf(200, 6) {
		t.Fatalf("read 2 and 3: %q", r.stdout)
	}
	for _, n := range []string{"2", "3"} {
		if r = h.must("", "verify-interval", stream, producer, n); !strings.Contains(r.stdout, "VERIFIED interval "+n) {
			t.Fatalf("verify %s:\n%s", n, r.stdout)
		}
	}
	step("read and verify-interval 2 and 3: both profiles")

	// The node prunes the anchors' fee tree: the host serves it by txid,
	// with its proof, and the audit does not need the node.
	node := h.env["BLOGS_ASSET"]
	h.env["BLOGS_ASSET"] = pruned(t)
	treeFrom := func(what, host string) string {
		t.Helper()
		r := h.must("", "verify-interval", stream, producer, "3", "-v")
		m := treeLine.FindStringSubmatch(r.stderr)
		if !strings.Contains(r.stdout, "VERIFIED interval 3") || m == nil || m[3] != host {
			t.Fatalf("%s:\n%s\n%s", what, r.stdout, r.stderr)
		}
		return m[2]
	}
	tree := treeFrom("verify with a pruned node", a.url)
	step("verify-interval 3 with a node that has pruned the fee tree: VERIFIED, the tree served by host a")

	// Host b missed everything: backfill it from a.
	h.env["BLOGS_HOSTS"] = b.url
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitIncomplete, "INCOMPLETE")
	for _, k := range []string{"1", "2", "3"} {
		r = h.must("", "backfill", stream, producer, "-from", a.url, "-to", b.url, "-interval", k)
		if !strings.Contains(r.stdout, "0 missing, 0 refused") {
			t.Fatalf("backfill %s:\n%s", k, r.stdout)
		}
		if k == "1" && !strings.Contains(r.stdout, "funding tree "+tree+" submitted from "+a.url) {
			t.Fatalf("backfill 1 did not carry the fee tree from the sibling:\n%s", r.stdout)
		}
	}
	treeFrom("verify at b", b.url)
	step("backfill: host b filled from host a (anchor chain, fee trees from host a, batches); verify-interval 3 VERIFIED at b, which serves the tree itself")

	// A restarted host answers from its restored index, the fully spent
	// fee tree included.
	a.stop()
	a.start()
	h.env["BLOGS_HOSTS"] = a.url
	r = h.must("", "read", stream, producer, "-content-dir", content)
	if !strings.HasPrefix(r.stdout, linesOf(0, 20)) {
		t.Fatalf("read after restart: %q", r.stdout)
	}
	treeFrom("verify after restart", a.url)
	h.env["BLOGS_ASSET"] = node
	step("host a restarted: read and verify-interval 3 from the restored index, the fee tree still served")

	// The facade goes away mid-publish: the carrier is persisted, and the
	// next run publishes those same bytes.
	a.stop()
	h.want(h.run("x\n", append([]string{"produce", stream}, fast...)...), exitUsage, "the carrier is persisted")
	a.start()
	r = h.must("", "anchor", stream, "-poll", "50ms")
	if !strings.Contains(r.stderr, "seq 11: publishing the carrier a previous run persisted") || !strings.Contains(r.stdout, "anchored to seq 11: interval 4") {
		t.Fatalf("anchor after a failed publish:\n%s\n%s", r.stdout, r.stderr)
	}
	r = h.must("", "read", stream, producer, "-from", "10", "-v", "-content-dir", content)
	if !strings.HasSuffix(r.stdout, "x\n") || strings.Contains(r.stderr, "BROKEN") {
		t.Fatalf("read after resume:\n%s\n%s", r.stdout, r.stderr)
	}
	h.must("", "verify-interval", stream, producer, "4")
	step("facade down mid-publish: exit 2 with the carrier persisted; anchor republished it, seq 11 linked, interval 4 VERIFIED")

	// Archives of both hosts hold the same proof of every interval, once b
	// is filled with interval 4, which only a received.
	h.must("", "backfill", stream, producer, "-from", a.url, "-to", b.url, "-interval", "4")
	dirA, dirB := t.TempDir(), t.TempDir()
	ra := h.must("", "archive", stream, producer, "-dir", dirA, "-host", a.url, "-content-dir", content)
	rb := h.must("", "archive", stream, producer, "-dir", dirB, "-host", b.url, "-content-dir", content)
	la, lb := latestLine.FindStringSubmatch(ra.stdout), latestLine.FindStringSubmatch(rb.stdout)
	if la == nil || lb == nil || la[1] != "4" || la[0] != lb[0] {
		t.Fatalf("archives:\n%s\n%s\n%s\n%s", ra.stdout, ra.stderr, rb.stdout, rb.stderr)
	}
	step("archive: both hosts' copies stored, intervals 1 to 4 proven with the same roots")
	if got, _ := filepath.Glob(filepath.Join(dirA, stream, producer, "trees", "*.beef")); len(got) == 0 || !strings.Contains(ra.stdout, "funding tree(s) with their proofs") {
		t.Fatalf("archive a kept no fee tree: %v\n%s", got, ra.stdout)
	}
	h.env["BLOGS_HOSTS"], h.env["BLOGS_ASSET"] = "", pruned(t)
	r = h.must("", "verify-interval", stream, producer, "4", "-archive", dirA, "-v")
	if !strings.Contains(r.stdout, "VERIFIED interval 4") || !strings.Contains(r.stderr, "from the archive ") {
		t.Fatalf("audit from archive a:\n%s\n%s", r.stdout, r.stderr)
	}
	h.env["BLOGS_ASSET"] = node
	step("archive a keeps the fee trees with their proofs; verify-interval 4 -archive VERIFIED from the files alone, no host and a pruned node")

	// A retraction on a real host: seq 7 (inside interval 2) swept. Host
	// b is off the plane (it was filled by backfill), and is in hosts, so
	// the sweep goes to it directly as well as to the facade.
	h.env["BLOGS_HOSTS"] = a.url + "," + b.url
	r = h.must("", "sweep", stream, "-retract-seqs", "7", "-yes", "-poll", "50ms")
	if !strings.Contains(r.stderr, "seqs 7 retracted") || strings.Contains(r.stderr, "MISSED") {
		t.Fatalf("sweep:\n%s\n%s", r.stdout, r.stderr)
	}
	h.env["BLOGS_HOSTS"] = b.url
	h.want(h.run("", "read", stream, producer, "-from", "7", "-to", "7"), exitIncomplete, "seq 7: MISSING")
	h.env["BLOGS_HOSTS"] = a.url
	h.want(h.run("", "read", stream, producer, "-from", "7", "-to", "7"), exitIncomplete, "seq 7: MISSING")
	r = h.must("", "archive", stream, producer, "-dir", t.TempDir(), "-host", a.url, "-content-dir", content)
	if !strings.Contains(r.stdout, "interval 2  proven-with-holes  seqs 6 to 8") || !strings.Contains(r.stdout, "holes 7") {
		t.Fatalf("archive after the retraction:\n%s\n%s", r.stdout, r.stderr)
	}
	step("sweep: seq 7 retracted at host a through the facade and at host b (off the plane) directly; a fresh archive proves interval 2 with the hole the anchor commits")

	// The archive that stored seq 7 keeps a receipt and drops its bytes;
	// the auditor proves interval 2 across the retracted seq.
	r = h.must("", "archive", stream, producer, "-dir", dirA, "-host", a.url, "-content-dir", content)
	if !strings.Contains(r.stdout, "retracted seqs 7: receipt and sweep kept") {
		t.Fatalf("archive recheck:\n%s\n%s", r.stdout, r.stderr)
	}
	if got, _ := filepath.Glob(filepath.Join(dirA, stream, producer, "batches", "0000000000000007-*")); len(got) != 0 {
		t.Fatalf("seq 7 bytes kept: %v", got)
	}
	r = h.must("", "verify-interval", stream, producer, "2")
	if !strings.Contains(r.stdout, "unserved seqs 7") || !strings.Contains(r.stdout, "VERIFIED interval 2") {
		t.Fatalf("verify across the retraction:\n%s", r.stdout)
	}
	step("archive a marks seq 7 retracted (receipt kept, bytes dropped); verify-interval 2 VERIFIED across it")

	r = h.must("", "doctor")
	if !strings.Contains(r.stdout, "host        "+a.url+" answering") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	step("doctor")
	t.Log("e2e client: PASS")
}

// The client end to end in unicast mode: no plane and no facade. The
// producer submits every carrier, tree and anchor to both reference hosts
// itself, and readers ask the hosts directly.
//
//	make e2e-unicast REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
func TestClientE2EUnicast(t *testing.T) {
	e2eSetup(t)
	h := newHarness(t)
	stream, producer := h.producer()
	step := func(what string) { t.Logf("ok  %s", what) }
	step("init, fund and stream new " + stream)
	hosts := refHosts(t, h.chainS.URL, stream, producer, "a", "b")
	a, b := hosts["a"], hosts["b"]
	delete(h.env, "BLOGS_FACADE")
	h.env["BLOGS_MODE"] = "unicast"
	h.env["BLOGS_HOSTS"] = a.url + "," + b.url
	step("two reference hosts; mode unicast, hosts a and b, no facade")

	// Interval 1, quorum all: both hosts take every object.
	r := h.produce(linesOf(0, 12), stream, "-max-lines", "4", "-poll", "50ms")
	if !strings.Contains(r.stdout, "published seq 1 to 3") || !strings.Contains(r.stdout, "anchored to seq 3: interval 1") ||
		!strings.Contains(r.stdout, "host "+a.url+": took") || !strings.Contains(r.stdout, "host "+b.url+": took") ||
		strings.Contains(r.stdout, "missed 1") || strings.Contains(r.stderr, "admitted nothing") {
		t.Fatalf("produce 1:\n%s\n%s", r.stdout, r.stderr)
	}
	step("produce: 3 batches and interval 1, every object submitted to both hosts, none missed")
	for _, host := range []*refHost{a, b} {
		r = h.must("", "-hosts", host.url, "read", stream, producer)
		if r.stdout != linesOf(0, 12) {
			t.Fatalf("read at %s: %q", host.name, r.stdout)
		}
		if r = h.must("", "-hosts", host.url, "verify-interval", stream, producer, "1"); !strings.Contains(r.stdout, "VERIFIED interval 1") {
			t.Fatalf("verify at %s:\n%s", host.name, r.stdout)
		}
	}
	step("read and verify-interval 1 at each host alone: VERIFIED at a and at b")

	// b goes down. Quorum all: the carrier is persisted, not published.
	b.stop()
	h.want(h.run("x\n", append([]string{"produce", stream}, fast...)...), exitUsage, "1 of 2 host(s) took it and the quorum is 2")
	step("b down, quorum all: exit 2, the carrier persisted, b named")

	// Quorum one: the persisted carrier and interval 2 go to a alone, and
	// the run names what b missed and the backfill that repairs it.
	r = h.must(linesOf(100, 4), append([]string{"-quorum", "one", "produce", stream, "-max-lines", "2"}, fast...)...)
	if !strings.Contains(r.stderr, "seq 4: publishing the carrier a previous run persisted") || !strings.Contains(r.stdout, "anchored to seq 6: interval 2") {
		t.Fatalf("quorum one:\n%s\n%s", r.stdout, r.stderr)
	}
	m := repairLine.FindStringSubmatch(r.stdout)
	if m == nil || !strings.Contains(r.stdout, "host "+b.url+": took 0 object(s), missed") {
		t.Fatalf("no tally or repair line:\n%s", r.stdout)
	}
	step("b down, quorum one: published at a, interval 2 anchored; b's misses listed with the repair command")

	// b is back, without what it missed; the printed command repairs it.
	b.start()
	h.want(h.run("", "-hosts", b.url, "verify-interval", stream, producer, "2"), exitIncomplete, "INCOMPLETE")
	r = h.must("", strings.Fields(m[1])...)
	if !strings.Contains(r.stdout, "backfill "+stream+" to "+b.url+": ") || !strings.Contains(r.stdout, "0 missing, 0 refused") {
		t.Fatalf("repair:\n%s", r.stdout)
	}
	if r = h.must("", "-hosts", b.url, "verify-interval", stream, producer, "2"); !strings.Contains(r.stdout, "VERIFIED interval 2") {
		t.Fatalf("verify at b after the repair:\n%s", r.stdout)
	}
	step("b back: INCOMPLETE at b, then the printed backfill; verify-interval 2 VERIFIED at b")

	// Both copies archived hold the same proof of every interval.
	dirA, dirB := t.TempDir(), t.TempDir()
	ra := h.must("", "archive", stream, producer, "-dir", dirA, "-host", a.url)
	rb := h.must("", "archive", stream, producer, "-dir", dirB, "-host", b.url)
	la, lb := latestLine.FindStringSubmatch(ra.stdout), latestLine.FindStringSubmatch(rb.stdout)
	if la == nil || lb == nil || la[1] != "2" || la[0] != lb[0] {
		t.Fatalf("archives:\n%s\n%s\n%s\n%s", ra.stdout, ra.stderr, rb.stdout, rb.stderr)
	}
	step("archive: a's and b's copies prove intervals 1 and 2 with the same roots")

	// A retraction reaches both hosts.
	r = h.must("", "sweep", stream, "-retract-seqs", "2", "-yes", "-poll", "50ms")
	if !strings.Contains(r.stderr, "seqs 2 retracted") {
		t.Fatalf("sweep:\n%s\n%s", r.stdout, r.stderr)
	}
	for _, host := range []*refHost{a, b} {
		h.want(h.run("", "-hosts", host.url, "read", stream, producer, "-from", "2", "-to", "2"), exitIncomplete, "seq 2: MISSING")
	}
	step("sweep: seq 2 retracted at both hosts")

	for i, host := range []*refHost{a, b} {
		dir := []string{dirA, dirB}[i]
		r = h.must("", "archive", stream, producer, "-dir", dir, "-host", host.url)
		if !strings.Contains(r.stdout, "retracted seqs 2: receipt and sweep kept") {
			t.Fatalf("archive recheck at %s:\n%s\n%s", host.url, r.stdout, r.stderr)
		}
		r = h.must("", "-hosts", host.url, "verify-interval", stream, producer, "1")
		if !strings.Contains(r.stdout, "unserved seqs 2") || !strings.Contains(r.stdout, "VERIFIED interval 1") {
			t.Fatalf("verify at %s:\n%s", host.url, r.stdout)
		}
	}
	step("both archives mark seq 2 retracted; verify-interval 1 VERIFIED across it at both hosts")

	r = h.must("", "doctor")
	if !strings.Contains(r.stdout, "mode        unicast") || !strings.Contains(r.stdout, "host        "+b.url+" answering") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	step("doctor")
	t.Log("e2e unicast: PASS")
}
