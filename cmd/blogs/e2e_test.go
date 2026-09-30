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

func TestClientE2E(t *testing.T) {
	dir := os.Getenv("BLOGS_E2E_REFERENCE_HOST")
	if dir == "" {
		t.Skip("set BLOGS_E2E_REFERENCE_HOST (make e2e-client REFERENCE_HOST=...)")
	}
	node := os.Getenv("BLOGS_E2E_NODE")
	if node == "" {
		node = "node"
	}
	// The bundle, staged as a deployment places it: in a modules/ directory
	// whose only node_modules is the reference host's.
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
	old := produce.Retries
	produce.Retries = []time.Duration{50 * time.Millisecond}
	defer func() { produce.Retries = old }()

	db := mysql(t)
	// The harness's chain is the one both hosts check proofs against; its
	// stand-in host is not used.
	h := newHarness(t)
	chainS := h.chainS
	stream, producer := h.producer()
	step := func(what string) { t.Logf("ok  %s", what) }
	step("init, fund and stream new " + stream)

	hosts := map[string]*refHost{}
	for _, name := range []string{"a", "b"} {
		port := freePort(t)
		rh := &refHost{t: t, dir: dir, node: node, name: name, port: port, db: db, url: fmt.Sprintf("http://127.0.0.1:%d", port)}
		rh.env = []string{
			"PATH=" + os.Getenv("PATH"),
			"OVERLAY_TOPICS=tm_log_" + stream,
			"OVERLAY_MODULES=" + module,
			"BLOGS_STREAMS=" + stream + "=" + producer,
			fmt.Sprintf("OVERLAY_KNEX_URL=mysql://root:%s@127.0.0.1:%d/overlay_%s", e2ePassword, db, name),
			"OVERLAY_CHAIN_TRACKER_URL=" + chainS.URL,
			"OVERLAY_ADMIN_TOKEN=blogs-e2e",
			"OVERLAY_LISTEN=127.0.0.1",
			"OVERLAY_PORT=" + strconv.Itoa(port),
		}
		rh.start()
		defer rh.stop()
		hosts[name] = rh
	}
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

	// Host b missed everything: backfill it from a.
	h.env["BLOGS_HOSTS"] = b.url
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitIncomplete, "INCOMPLETE")
	for _, k := range []string{"1", "2", "3"} {
		r = h.must("", "backfill", stream, producer, "-from", a.url, "-to", b.url, "-interval", k)
		if !strings.Contains(r.stdout, "0 missing, 0 refused") {
			t.Fatalf("backfill %s:\n%s", k, r.stdout)
		}
	}
	r = h.must("", "verify-interval", stream, producer, "3")
	if !strings.Contains(r.stdout, "VERIFIED interval 3") {
		t.Fatalf("verify at b:\n%s", r.stdout)
	}
	step("backfill: host b filled from host a (anchor chain, fee trees from the node, batches); verify-interval 3 VERIFIED at b")

	// A restarted host answers from its restored index.
	a.stop()
	a.start()
	h.env["BLOGS_HOSTS"] = a.url
	r = h.must("", "read", stream, producer, "-content-dir", content)
	if !strings.HasPrefix(r.stdout, linesOf(0, 20)) {
		t.Fatalf("read after restart: %q", r.stdout)
	}
	h.must("", "verify-interval", stream, producer, "3")
	step("host a restarted: read and verify-interval 3 from the restored index")

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

	// A retraction on a real host: seq 7 (inside interval 2) swept.
	r = h.must("", "sweep", stream, "-retract-seqs", "7", "-yes", "-poll", "50ms")
	if !strings.Contains(r.stderr, "seqs 7 retracted") {
		t.Fatalf("sweep:\n%s\n%s", r.stdout, r.stderr)
	}
	h.want(h.run("", "read", stream, producer, "-from", "7", "-to", "7"), exitIncomplete, "seq 7: MISSING")
	r = h.must("", "archive", stream, producer, "-dir", t.TempDir(), "-host", a.url, "-content-dir", content)
	if !strings.Contains(r.stdout, "interval 2  proven-with-holes  seqs 6 to 8") || !strings.Contains(r.stdout, "holes 7") {
		t.Fatalf("archive after the retraction:\n%s\n%s", r.stdout, r.stderr)
	}
	step("sweep: seq 7 retracted at host a; a fresh archive proves interval 2 with the hole the anchor commits")

	r = h.must("", "doctor")
	if !strings.Contains(r.stdout, "host        "+a.url+" answering") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	step("doctor")
	t.Log("e2e client: PASS")
}
