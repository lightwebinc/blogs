package state

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
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

// A state file written before trees were recorded as they are signed has
// neither list, and loads; a file saved without records has neither key.
func TestAStateFileWithoutTreeRecordsLoads(t *testing.T) {
	home := t.TempDir()
	dir := Dir(home, "app_abcdefghij")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := `{
  "version": 1,
  "stream": "app_abcdefghij",
  "identity": "02ab",
  "seq": 2,
  "head": "cc",
  "unanchored": ["bb", "cc"],
  "nextFirst": 1,
  "tree": {"identityKeyHex": "02ab", "txid": "aa", "rawHex": "00", "sats": 1, "count": 4, "next": 2},
  "trees": [{"identityKeyHex": "02ab", "txid": "aa", "rawHex": "00", "sats": 1, "count": 4, "next": 2}],
  "ahead": [{"identityKeyHex": "02ab", "txid": "dd", "rawHex": "00", "sats": 1, "count": 4}]
}
`
	file := filepath.Join(dir, "state.json")
	if err := os.WriteFile(file, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Load(home, "app_abcdefghij")
	if err != nil {
		t.Fatalf("an earlier state file: %v", err)
	}
	if s.Seq != 2 || s.Tree == nil || s.Tree.Next != 2 || len(s.Ahead) != 1 || len(s.PendingTrees) != 0 || len(s.Unpublished) != 0 {
		t.Fatalf("%+v", s)
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(file)
	if strings.Contains(string(raw), "pendingTrees") || strings.Contains(string(raw), "unpublished") {
		t.Fatalf("a state without records names them:\n%s", raw)
	}
}

func TestPendingTreeRecordsAreKeyedByTxidAndSaved(t *testing.T) {
	home := t.TempDir()
	s, err := Create(home, "app_abcdefghij", "02ab")
	if err != nil {
		t.Fatal(err)
	}
	coin := bwallet.Output{TxID: "c0", Vout: 1, Satoshis: 5000}
	s.KeepPendingTree(PendingTree{Kind: "batch", Tree: funding.Tree{Txid: "aa", Count: 4, Sats: 1}, Coin: coin})
	s.KeepPendingTree(PendingTree{Kind: "anchor", Tree: funding.Tree{Txid: "bb", Count: 3, Sats: 1000}, Coin: bwallet.Output{TxID: "c1"}})
	// The same tree again replaces its record.
	s.KeepPendingTree(PendingTree{Kind: "batch", Tree: funding.Tree{Txid: "aa", Count: 4, Sats: 1}, Coin: coin, ReturnedOnce: true})
	s.Unpublished = []string{"ee"}
	s.Ahead = []funding.Tree{{Txid: "bb"}}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(Dir(home, "app_abcdefghij"), "state.json")
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
	raw, _ := os.ReadFile(file)
	for _, key := range []string{`"pendingTrees"`, `"kind": "batch"`, `"tree"`, `"coin"`, `"returned_once": true`, `"unpublished"`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("no %s in:\n%s", key, raw)
		}
	}
	back, err := Load(home, "app_abcdefghij")
	if err != nil {
		t.Fatal(err)
	}
	if len(back.PendingTrees) != 2 || back.PendingTrees[0].Tree.Txid != "aa" || back.PendingTrees[1].Kind != "anchor" {
		t.Fatalf("%+v", back.PendingTrees)
	}
	p := back.PendingTree("aa")
	if p == nil || !p.ReturnedOnce || p.Coin.Outpoint() != coin.Outpoint() || p.Tree.Count != 4 {
		t.Fatalf("%+v", p)
	}
	if back.PendingTree("zz") != nil || !back.MintedAhead("bb") || back.MintedAhead("aa") {
		t.Fatal("lookups")
	}
	if !back.DropPendingTree("aa") || back.DropPendingTree("aa") || len(back.PendingTrees) != 1 || back.PendingTrees[0].Tree.Txid != "bb" {
		t.Fatalf("after a drop: %+v", back.PendingTrees)
	}
	if len(back.Unpublished) != 1 || back.Unpublished[0] != "ee" {
		t.Fatalf("%v", back.Unpublished)
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
