package vectors

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/lightwebinc/blogs/internal/logrec"
)

type queryJSON struct {
	Name string `json:"name"`
	// Query is the question's query object as JSON text, so that a member
	// given twice or a number written as a double reaches each parser as
	// written.
	Query string `json:"query"`
	// Class is the class a host answers it as; empty when it is refused.
	Class   string `json:"class,omitempty"`
	Verdict string `json:"verdict"`
	// Parsed is what the host reads from an accepted question.
	Parsed *parsedQueryJSON `json:"parsed,omitempty"`
}

type parsedQueryJSON struct {
	Stream   string `json:"stream"`
	Producer string `json:"producer,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
	Interval uint64 `json:"interval,omitempty"`
	Tree     string `json:"tree,omitempty"`
}

// queryVectors are ls_log questions (docs/spec.md section 7.2): each class
// accepted, and what refuses a question. The verdict is the Go codec's; the
// generator fails if it is not the one the case states.
func queryVectors(prod []byte) (map[string]any, error) {
	p := h(prod)
	tree := sha256.Sum256([]byte("blogs/vector/tree-txid"))
	t := h(tree[:])
	q := func(members ...string) string { return "{" + strings.Join(members, ",") + "}" }
	s := `"stream":"` + Stream + `"`
	pr := `"producer":"` + p + `"`
	tr := `"tree":"` + t + `"`
	cases := []struct {
		name, query, class string
	}{
		{"batch", q(s, pr, `"seq":1`), logrec.ClassBatch},
		{"batch-seq-as-double", q(s, pr, `"seq":7.0`), logrec.ClassBatch},
		{"batch-seq-largest", q(s, pr, `"seq":9007199254740991`), logrec.ClassBatch},
		{"batch-member-twice-last-wins", q(s, pr, `"seq":9`, `"seq":2`), logrec.ClassBatch},
		{"interval", q(pr, `"interval":2`, s), logrec.ClassInterval},
		{"tree", q(s, tr), logrec.ClassTree},
		{"tree-members-in-any-order", q(tr, s), logrec.ClassTree},

		{"not-an-object", `[]`, ""},
		{"null", `null`, ""},
		{"empty", q(), ""},
		{"no-class-member", q(s, pr), ""},
		{"batch-without-producer", q(s, `"seq":1`), ""},
		{"batch-without-stream", q(pr, `"seq":1`), ""},
		{"batch-and-interval", q(s, pr, `"seq":1`, `"interval":1`), ""},
		{"batch-unknown-member", q(s, pr, `"seq":1`, `"extra":true`), ""},
		{"stream-readable-part-alone", q(`"stream":"example_app"`, pr, `"seq":1`), ""},
		{"stream-upper-case", q(`"stream":"Example_app_qzxkvbmwtr"`, pr, `"seq":1`), ""},
		{"stream-not-a-string", q(`"stream":7`, pr, `"seq":1`), ""},
		{"producer-upper-case", q(s, `"producer":"`+strings.ToUpper(p)+`"`, `"seq":1`), ""},
		{"producer-short", q(s, `"producer":"`+p[2:]+`"`, `"seq":1`), ""},
		{"producer-not-on-curve", q(s, `"producer":"02`+strings.Repeat("ff", 32)+`"`, `"seq":1`), ""},
		{"seq-zero", q(s, pr, `"seq":0`), ""},
		{"seq-fraction", q(s, pr, `"seq":1.5`), ""},
		{"seq-over-2-53", q(s, pr, `"seq":9007199254740992`), ""},
		{"seq-a-string", q(s, pr, `"seq":"1"`), ""},
		{"seq-negative", q(s, pr, `"seq":-1`), ""},
		{"interval-zero", q(s, pr, `"interval":0`), ""},
		{"tree-with-producer", q(s, pr, tr), ""},
		{"tree-with-seq", q(s, tr, `"seq":1`), ""},
		{"tree-with-interval", q(s, pr, tr, `"interval":1`), ""},
		{"tree-with-output", q(s, tr, `"vout":0`), ""},
		{"tree-without-stream", q(tr), ""},
		{"tree-upper-case", q(s, `"tree":"`+strings.ToUpper(t)+`"`), ""},
		{"tree-short", q(s, `"tree":"`+t[2:]+`"`), ""},
		{"tree-not-hex", q(s, `"tree":"`+strings.Repeat("zz", 32)+`"`), ""},
		{"tree-not-a-string", q(s, `"tree":1`), ""},
		{"tree-stream-readable-part-alone", q(`"stream":"example_app"`, tr), ""},
	}
	var out []queryJSON
	for _, c := range cases {
		got, err := logrec.ParseQuery([]byte(c.query))
		j := queryJSON{Name: c.name, Query: c.query, Class: c.class, Verdict: "refuse"}
		switch {
		case c.class == "" && err == nil:
			return nil, fmt.Errorf("query vector %s: accepted as %s, want refused", c.name, got.Class)
		case c.class != "" && err != nil:
			return nil, fmt.Errorf("query vector %s: %w", c.name, err)
		case c.class != "" && got.Class != c.class:
			return nil, fmt.Errorf("query vector %s: class %s, want %s", c.name, got.Class, c.class)
		case c.class != "":
			j.Verdict = "answer"
			j.Parsed = &parsedQueryJSON{Stream: got.Stream, Producer: h(got.Producer), Seq: got.Seq, Interval: got.Interval, Tree: got.Tree}
		}
		out = append(out, j)
	}
	return map[string]any{
		"description": "Questions to the lookup service (docs/spec.md section 7.2). query is the question's query object as JSON text; a host parses it and validates the parsed value. Three classes, all free: batch {stream, producer, seq}, interval {stream, producer, interval} and tree {stream, tree}. A question carries exactly the members of one class; anything else is refused with an error, never answered empty and never ignored. The tree txid is a stand-in.",
		"service":     logrec.LookupService,
		"classes": map[string]any{
			logrec.ClassBatch:    []string{"stream", "producer", "seq"},
			logrec.ClassInterval: []string{"stream", "producer", "interval"},
			logrec.ClassTree:     []string{"stream", "tree"},
		},
		"queries": out,
	}, nil
}
