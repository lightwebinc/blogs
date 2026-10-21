package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Loki push (POST /loki/api/v1/push): logproto.PushRequest as protobuf
// (snappy-compressed by every client that speaks it) or as JSON. A
// stream's labels are the record's labels, an entry's structured metadata
// its attributes, and its line the body. Field numbers and wire types are
// Loki v3.7.9's pkg/push/push.proto (TestLokiUpstreamFields).

// DecodeLokiProto reads a PushRequest in protobuf (already decompressed).
func DecodeLokiProto(b []byte) ([]Record, error) {
	var out []Record
	err := each(b, func(p *pbuf, num, wt int) error {
		if num != 1 {
			return errSkip
		}
		sb, err := p.lenField(wt)
		if err != nil {
			return err
		}
		var labels map[string]string
		var entries [][]byte
		if err := each(sb, func(p *pbuf, num, wt int) error {
			switch num {
			case 1:
				s, err := p.lenField(wt)
				if err != nil {
					return err
				}
				labels, err = ParseLabels(string(s))
				return err
			case 2:
				e, err := p.lenField(wt)
				entries = append(entries, e)
				return err
			}
			return errSkip
		}); err != nil {
			return err
		}
		for _, eb := range entries {
			rec := Record{Labels: labels}
			attrs := map[string]string{}
			if err := each(eb, func(p *pbuf, num, wt int) error {
				switch num {
				case 1:
					tb, err := p.lenField(wt)
					if err != nil {
						return err
					}
					var sec, nanos uint64
					if err := each(tb, func(p *pbuf, num, wt int) error {
						var err error
						switch num {
						case 1:
							sec, err = p.varField(wt)
						case 2:
							nanos, err = p.varField(wt)
						default:
							return errSkip
						}
						return err
					}); err != nil {
						return err
					}
					if sec > uint64(math.MaxInt64/1_000_000_000) || nanos >= 1e9 {
						return errWire
					}
					rec.Ts = int64(sec)*1e9 + int64(nanos) //nolint:gosec // bounded above
					return nil
				case 2:
					s, err := p.lenField(wt)
					rec.Body = string(s)
					return err
				case 3:
					pb, err := p.lenField(wt)
					if err != nil {
						return err
					}
					var k, v string
					if err := each(pb, func(p *pbuf, num, wt int) error {
						if num != 1 && num != 2 {
							return errSkip
						}
						s, err := p.lenField(wt)
						if num == 1 {
							k = string(s)
						} else {
							v = string(s)
						}
						return err
					}); err != nil {
						return err
					}
					attrs[k] = v
					return nil
				}
				return errSkip
			}); err != nil {
				return err
			}
			if len(attrs) > 0 {
				rec.Attrs = attrs
			}
			rec.Ts = orNow(rec.Ts)
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

// DecodeLokiJSON reads a push in Loki's JSON form: values are
// [ts, line] or [ts, line, {structured metadata}].
func DecodeLokiJSON(b []byte) ([]Record, error) {
	var req struct {
		Streams []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(b, &req); err != nil {
		return nil, err
	}
	var out []Record
	for _, s := range req.Streams {
		for _, v := range s.Values {
			if len(v) < 2 || len(v) > 3 {
				return nil, errors.New("a value is not [ts, line] or [ts, line, metadata]")
			}
			var ts, line string
			if err := json.Unmarshal(v[0], &ts); err != nil {
				return nil, fmt.Errorf("value timestamp: %w", err)
			}
			if err := json.Unmarshal(v[1], &line); err != nil {
				return nil, fmt.Errorf("value line: %w", err)
			}
			n, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("value timestamp %q: %w", ts, err)
			}
			rec := Record{Ts: orNow(n), Body: line, Labels: s.Stream}
			if len(v) == 3 {
				var md map[string]string
				if err := json.Unmarshal(v[2], &md); err != nil {
					return nil, fmt.Errorf("value metadata: %w", err)
				}
				if len(md) > 0 {
					rec.Attrs = md
				}
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

// ParseLabels reads a label set in Prometheus form: {a="b", c="d"}.
func ParseLabels(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("labels %q are not {...}", s)
	}
	s = s[1 : len(s)-1]
	m := map[string]string{}
	for {
		s = strings.TrimLeft(s, " ,")
		if s == "" {
			return m, nil
		}
		eq := strings.IndexByte(s, '=')
		if eq < 1 {
			return nil, errors.New("labels: a name without a value")
		}
		name := strings.TrimSpace(s[:eq])
		s = strings.TrimSpace(s[eq+1:])
		if !strings.HasPrefix(s, `"`) {
			return nil, errors.New("labels: a value is not quoted")
		}
		var b strings.Builder
		i := 1
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i])
				}
				continue
			}
			b.WriteByte(s[i])
		}
		if i >= len(s) {
			return nil, errors.New("labels: an unterminated value")
		}
		m[name] = b.String()
		s = s[i+1:]
	}
}
