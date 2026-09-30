package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateLoadSave(t *testing.T) {
	home := t.TempDir()
	s, err := Create(home, "app_abcdefghij", "02ab")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(home, "app_abcdefghij", "02ab"); !errors.Is(err, ErrExists) {
		t.Fatalf("a second create: %v", err)
	}
	s.Seq, s.Head, s.Unanchored = 1, "cc", []string{"cc"}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(filepath.Join(Dir(home, "app_abcdefghij"), "state.json"))
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
	back, err := Load(home, "app_abcdefghij")
	if err != nil || back.Seq != 1 || back.NextFirst != 1 {
		t.Fatalf("%+v %v", back, err)
	}
	// A state whose unanchored commitments do not reach its seq is refused.
	back.Unanchored = nil
	if err := back.Save(); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(home, "app_abcdefghij"); err == nil {
		t.Fatal("an inconsistent state loaded")
	}
	if _, err := Load(home, "app_zzzzzzzzzz"); !errors.Is(err, ErrNoStream) {
		t.Fatalf("no stream: %v", err)
	}
	names, err := List(home)
	if err != nil || len(names) != 1 {
		t.Fatalf("%v %v", names, err)
	}
	left, _ := filepath.Glob(filepath.Join(Dir(home, "app_abcdefghij"), ".tmp-*"))
	if len(left) != 0 {
		t.Fatalf("temporary files left: %v", left)
	}
}

func TestLockIsExclusive(t *testing.T) {
	p := filepath.Join(t.TempDir(), "lock")
	unlock, err := Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(p); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	unlock()
	again, err := Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	again()
}
