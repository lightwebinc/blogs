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
	for k, v := range map[string]string{"network": "mainnet", "timeout": "-1s", "object_bound": "0"} {
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
