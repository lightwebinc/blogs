package logrec

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// The ls_log question classes (docs/spec.md section 7.2), all free.
const (
	ClassBatch    = "batch"
	ClassInterval = "interval"
	ClassTree     = "tree"
)

// Query is one ls_log question, validated: exactly one class's members.
type Query struct {
	Class  string
	Stream string
	// Producer is the identity key, set for the batch and interval classes.
	Producer []byte
	// Seq is set for the batch class, Interval for the interval class.
	Seq, Interval uint64
	// Tree is the funding tree's txid, 64 lowercase hex characters
	// (display order), set for the tree class.
	Tree string
}

// CheckTxid holds s to the form a question names a transaction by: 64
// lowercase hex characters.
func CheckTxid(s string) error {
	if len(s) != 64 || strings.ToLower(s) != s {
		return errors.New("not a txid of 64 lowercase hex characters")
	}
	if _, err := hex.DecodeString(s); err != nil {
		return errors.New("not a txid of 64 lowercase hex characters")
	}
	return nil
}

// ParseQuery validates a question's query object as parsed (docs/spec.md
// section 7.2): exactly the members of one class, each within its rule. A
// member given twice takes its last value, as JSON parsers give it. Anything
// else is an error, never an empty answer and never ignored.
func ParseQuery(raw []byte) (*Query, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("ls_log: the query is not JSON: %w", err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("ls_log: the query is not an object")
	}
	var members []string
	for k := range m {
		members = append(members, k)
	}
	sort.Strings(members)
	_, isTree := m["tree"]
	_, isSeq := m["seq"]
	q := &Query{Class: ClassInterval}
	want := []string{"interval", "producer", "stream"}
	switch {
	case isTree:
		q.Class, want = ClassTree, []string{"stream", "tree"}
	case isSeq:
		q.Class, want = ClassBatch, []string{"producer", "seq", "stream"}
	}
	if strings.Join(members, ",") != strings.Join(want, ",") {
		got := strings.Join(members, ", ")
		if got == "" {
			got = "nothing"
		}
		return nil, fmt.Errorf("ls_log: a query has exactly stream, producer and one of seq or interval, or exactly stream and tree; got %s", got)
	}
	if q.Stream, ok = m["stream"].(string); !ok {
		return nil, errors.New("ls_log: stream is not a string")
	}
	if CheckStream(q.Stream) != nil {
		return nil, errors.New("ls_log: stream is not a stream identifier <name>_<suffix>")
	}
	if q.Class == ClassTree {
		if q.Tree, ok = m["tree"].(string); !ok || CheckTxid(q.Tree) != nil {
			return nil, errors.New("ls_log: tree is not a txid of 64 lowercase hex characters")
		}
		return q, nil
	}
	p, ok := m["producer"].(string)
	if !ok || len(p) != 66 || strings.ToLower(p) != p {
		return nil, errors.New("ls_log: producer is not 66 lowercase hex characters")
	}
	var err error
	if q.Producer, err = hex.DecodeString(p); err != nil {
		return nil, errors.New("ls_log: producer is not 66 lowercase hex characters")
	}
	if CheckProducer(q.Producer) != nil {
		return nil, errors.New("ls_log: producer is not a compressed secp256k1 key")
	}
	member := "interval"
	if q.Class == ClassBatch {
		member = "seq"
	}
	n, ok := m[member].(float64)
	if !ok || n != math.Trunc(n) || n < 1 || n > MaxSafe {
		return nil, fmt.Errorf("ls_log: %s is not an integer from 1 to 2^53 - 1", member)
	}
	if q.Class == ClassBatch {
		q.Seq = uint64(n)
	} else {
		q.Interval = uint64(n)
	}
	return q, nil
}
