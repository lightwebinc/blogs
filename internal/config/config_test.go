package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileEnvironmentAndFlagsLayer(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config")
	file := "# a producer\nfacade = http://192.0.2.10:8080\nhosts = http://192.0.2.10:8080, http://192.0.2.11:8080\nnetwork = test\ntimeout = 5s\n"
	if err := os.WriteFile(p, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"BLOGS_NETWORK": "regtest", "BLOGS_OBJECT_BOUND": "2048"}
	c, err := Load(p, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.Facade != "http://192.0.2.10:8080" || len(c.Hosts) != 2 || c.Hosts[1] != "http://192.0.2.11:8080" ||
		c.Network != "regtest" || c.Timeout != 5*time.Second || c.ObjectBound != 2048 || c.Originator != "blogs" {
		t.Fatalf("%+v", c)
	}
	c, err = c.Apply(map[string]string{"network": "main"})
	if err != nil || c.Network != "main" {
		t.Fatalf("flag over environment: %v %s", err, c.Network)
	}
	if _, err := Load(filepath.Join(dir, "absent"), func(string) string { return "" }); err != nil {
		t.Fatalf("a missing file is not an error: %v", err)
	}
}

func TestTheGrammarRefuses(t *testing.T) {
	for _, bad := range []string{"facde = x\n", "no equals\n", "facade = a\nfacade = b\n"} {
		if _, err := Parse(strings.NewReader(bad)); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
	if _, err := Parse(strings.NewReader("facde = x\n")); !errors.Is(err, ErrUnknownKey) {
		t.Error("an unknown key is not ErrUnknownKey")
	}
	for k, v := range map[string]string{"network": "mainnet", "timeout": "-1s", "object_bound": "0", "tx_source": "woc:regtest"} {
		if _, err := Defaults().Apply(map[string]string{k: v}); err == nil {
			t.Errorf("%s = %s applied", k, v)
		}
	}
}

func TestPath(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	if Path("/x/c", get) != "/x/c" {
		t.Error("the flag wins")
	}
	env["XDG_CONFIG_HOME"] = "/xdg"
	if Path("", get) != "/xdg/blogs/config" {
		t.Error(Path("", get))
	}
	env["BLOGS_HOME"] = "/h"
	if Path("", get) != "/h/config" {
		t.Error(Path("", get))
	}
}

func TestModeAndQuorum(t *testing.T) {
	c := Defaults()
	if c.Mode != ModePlane || c.Quorum != QuorumAll {
		t.Fatalf("defaults: mode %q quorum %q", c.Mode, c.Quorum)
	}
	c, err := c.Apply(map[string]string{"mode": "unicast", "quorum": "majority"})
	if err != nil || c.Mode != ModeUnicast || c.Quorum != QuorumMajority {
		t.Fatalf("%v %+v", err, c)
	}
	for k, v := range map[string]string{"mode": "multicast", "quorum": "most"} {
		if _, err := Defaults().Apply(map[string]string{k: v}); err == nil {
			t.Errorf("%s = %s applied", k, v)
		}
	}
	for _, q := range []string{"0", "-1", "two"} {
		if _, err := Defaults().Apply(map[string]string{"quorum": q}); err == nil {
			t.Errorf("quorum %s applied", q)
		}
	}
	for _, tc := range []struct {
		q    string
		n    int
		need int
	}{
		{"all", 3, 3}, {"majority", 1, 1}, {"majority", 2, 2}, {"majority", 3, 2}, {"majority", 4, 3},
		{"one", 5, 1}, {"2", 3, 2}, {"3", 3, 3},
	} {
		if got, err := Need(tc.q, tc.n); err != nil || got != tc.need {
			t.Errorf("Need(%s, %d) = %d, %v; want %d", tc.q, tc.n, got, err, tc.need)
		}
	}
	if _, err := Need("4", 3); err == nil {
		t.Error("a quorum above the host count is accepted")
	}
	if _, err := Need("all", 0); err == nil {
		t.Error("no hosts is accepted")
	}
}

func TestLimits(t *testing.T) {
	many := strings.TrimSuffix(strings.Repeat("http://192.0.2.1:8080,", 17), ",")
	for k, v := range map[string]string{"hosts": many, "object_bound": "8388609", "timeout": "500ms"} {
		_, err := Defaults().Apply(map[string]string{k: v})
		if err == nil || !strings.Contains(err.Error(), "limit") {
			t.Errorf("%s = %s: %v", k, v, err)
		}
	}
	for k, v := range map[string]string{"object_bound": "8388608", "timeout": "5m"} {
		if _, err := Defaults().Apply(map[string]string{k: v}); err != nil {
			t.Errorf("%s = %s at the limit: %v", k, v, err)
		}
	}
}
