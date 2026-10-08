// Package ingest is the provenance sidecar's engine: it turns what log
// shippers send (syslog over TCP or TLS, OTLP logs over HTTP, Loki push)
// into records, writes each record as one line in the line form, batches
// the lines onto blogs streams through the producer engine, and forwards
// each published line, with where it was published, to a log store.
//
// Nothing here changes the blogs record: a line is content, and the line
// form is a convention of the sidecar, versioned by its own field
// (docs/backend.md).
package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FormVersion is the line form's version, the "v" of every line.
const FormVersion = 1

// MaxRecordLine is the longest line form a record may encode to, newline
// excluded. The producer carries a longer input line as several lines,
// which would break the one record, one line rule, so a record over it is
// refused instead.
const MaxRecordLine = 64*1024 - 1

// Record is one log record from any door.
type Record struct {
	// Ts is the record's own time in nanoseconds since the epoch: a claim
	// of the shipper, like a batch's timestamp.
	Ts int64
	// Sev is the severity as text, or empty.
	Sev string
	// Labels are the low-cardinality keys a store indexes.
	Labels map[string]string
	// Attrs are everything else: attributes, structured metadata, fields.
	Attrs map[string]string
	Body  string
}

// form is the line form. Its fields are in key order and the encoder adds
// no space, so a line is the same bytes wherever it is built.
type form struct {
	Attrs  map[string]string `json:"attrs,omitempty"`
	Body   string            `json:"body"`
	Labels map[string]string `json:"labels"`
	Sev    string            `json:"sev,omitempty"`
	Ts     string            `json:"ts"`
	V      int               `json:"v"`
}

// Line is the record in the line form, with no trailing newline.
func (r *Record) Line() ([]byte, error) {
	labels := r.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(form{Attrs: r.Attrs, Body: r.Body, Labels: labels, Sev: r.Sev, Ts: strconv.FormatInt(r.Ts, 10), V: FormVersion}); err != nil {
		return nil, err
	}
	b := bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
	if len(b) > MaxRecordLine {
		return nil, fmt.Errorf("a record encodes to %d bytes, over the line bound of %d", len(b), MaxRecordLine)
	}
	return b, nil
}

// ParseLine reads a line in the line form back: the labels and timestamp a
// read gateway compares with what a store answers.
func ParseLine(b []byte) (*Record, error) {
	var f form
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	if f.V != FormVersion {
		return nil, fmt.Errorf("line form version %d is not %d", f.V, FormVersion)
	}
	ts, err := strconv.ParseInt(f.Ts, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("line form ts %q: %w", f.Ts, err)
	}
	return &Record{Ts: ts, Sev: f.Sev, Labels: f.Labels, Attrs: f.Attrs, Body: f.Body}, nil
}

// LabelName makes s a valid store label name: [a-zA-Z_][a-zA-Z0-9_]*, with
// every other character an underscore.
func LabelName(s string) string {
	var b strings.Builder
	for i, c := range s {
		ok := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9')
		if ok {
			b.WriteRune(c)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

// sortedKeys returns m's keys in order.
func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// orNow is ts, or now when ts is zero.
func orNow(ts int64) int64 {
	if ts == 0 {
		return time.Now().UnixNano()
	}
	return ts
}
