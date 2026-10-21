package ingest

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// OTLP logs (opentelemetry-proto, opentelemetry/proto/collector/logs/v1):
// an ExportLogsServiceRequest, as protobuf or as OTLP's JSON mapping.
//
// Mapping to a record: the resource's service.name is the label
// service_name; every other resource attribute is an attribute prefixed
// "resource.", the scope's name is "scope", a record's attributes are
// attributes as they are, its trace and span ids are trace_id and span_id
// in hex, its severity text (or, without one, the name of its severity
// number) is the severity, and its body is the body: a string as it is,
// anything else as JSON; its event name, when set, is the attribute
// event_name. A record with no time takes its observed time.
//
// Field numbers and wire types are opentelemetry-proto v1.11.1's
// (TestOTLPUpstreamFields covers every field of the messages read).
// AnyValue.string_value_strindex and KeyValue.key_strindex are defined for
// the profiles signal only, and a logs reader treats them as absent.

// severityNames are OTLP's severity numbers 1 to 24, by range.
var severityNames = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

func otlpSeverity(n int64) string {
	if n < 1 || n > 24 {
		return ""
	}
	return severityNames[(n-1)/4]
}

type otlpResource struct {
	labels map[string]string
	attrs  map[string]string
}

func (r otlpResource) record(scope string, ts, observed int64, sevText string, sevNum int64, body string, attrs map[string]string, trace, span []byte, event string) Record {
	rec := Record{Ts: ts, Body: body, Labels: map[string]string{}, Attrs: map[string]string{}}
	if rec.Ts == 0 {
		rec.Ts = observed
	}
	rec.Ts = orNow(rec.Ts)
	for k, v := range r.labels {
		rec.Labels[k] = v
	}
	for k, v := range r.attrs {
		rec.Attrs[k] = v
	}
	if scope != "" {
		rec.Attrs["scope"] = scope
	}
	for k, v := range attrs {
		rec.Attrs[k] = v
	}
	if len(trace) > 0 {
		rec.Attrs["trace_id"] = hex.EncodeToString(trace)
	}
	if len(span) > 0 {
		rec.Attrs["span_id"] = hex.EncodeToString(span)
	}
	if event != "" {
		rec.Attrs["event_name"] = event
	}
	rec.Sev = sevText
	if rec.Sev == "" {
		rec.Sev = otlpSeverity(sevNum)
	}
	if len(rec.Attrs) == 0 {
		rec.Attrs = nil
	}
	return rec
}

func newResource(attrs map[string]string) otlpResource {
	r := otlpResource{labels: map[string]string{}, attrs: map[string]string{}}
	for k, v := range attrs {
		if k == "service.name" {
			r.labels["service_name"] = v
			continue
		}
		r.attrs["resource."+k] = v
	}
	if r.labels["service_name"] == "" {
		r.labels["service_name"] = "unknown_service"
	}
	return r
}

// DecodeOTLPProto reads an ExportLogsServiceRequest in protobuf.
func DecodeOTLPProto(b []byte) ([]Record, error) {
	var out []Record
	err := each(b, func(p *pbuf, num, wt int) error {
		if num != 1 {
			return errSkip
		}
		rl, err := p.lenField(wt)
		if err != nil {
			return err
		}
		recs, err := otlpResourceLogs(rl)
		out = append(out, recs...)
		return err
	})
	return out, err
}

func otlpResourceLogs(b []byte) ([]Record, error) {
	var res otlpResource
	var scopes [][]byte
	err := each(b, func(p *pbuf, num, wt int) error {
		switch num {
		case 1:
			rb, err := p.lenField(wt)
			if err != nil {
				return err
			}
			attrs := map[string]string{}
			if err := each(rb, func(p *pbuf, num, wt int) error {
				if num != 1 {
					return errSkip
				}
				kv, err := p.lenField(wt)
				if err != nil {
					return err
				}
				return pbKeyValue(kv, attrs)
			}); err != nil {
				return err
			}
			res = newResource(attrs)
			return nil
		case 2:
			sb, err := p.lenField(wt)
			scopes = append(scopes, sb)
			return err
		}
		return errSkip
	})
	if err != nil {
		return nil, err
	}
	if res.labels == nil {
		res = newResource(nil)
	}
	var out []Record
	for _, sb := range scopes {
		var scope string
		var logs [][]byte
		if err := each(sb, func(p *pbuf, num, wt int) error {
			switch num {
			case 1:
				ib, err := p.lenField(wt)
				if err != nil {
					return err
				}
				return each(ib, func(p *pbuf, num, wt int) error {
					if num != 1 {
						return errSkip
					}
					n, err := p.lenField(wt)
					scope = string(n)
					return err
				})
			case 2:
				lb, err := p.lenField(wt)
				logs = append(logs, lb)
				return err
			}
			return errSkip
		}); err != nil {
			return nil, err
		}
		for _, lb := range logs {
			rec, err := pbLogRecord(lb, res, scope)
			if err != nil {
				return nil, err
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

func pbLogRecord(b []byte, res otlpResource, scope string) (Record, error) {
	var ts, observed, sevNum int64
	var sevText, body, event string
	var trace, span []byte
	attrs := map[string]string{}
	err := each(b, func(p *pbuf, num, wt int) error {
		switch num {
		case 1, 11:
			if err := want(wt, 1); err != nil {
				return err
			}
			v, err := p.fixed64()
			if v > math.MaxInt64 {
				return errWire
			}
			if num == 1 {
				ts = int64(v)
			} else {
				observed = int64(v)
			}
			return err
		case 2:
			v, err := p.varField(wt)
			sevNum = int64(v) //nolint:gosec // an enum; out of range is no name
			return err
		case 3:
			v, err := p.lenField(wt)
			sevText = string(v)
			return err
		case 5:
			v, err := p.lenField(wt)
			if err != nil {
				return err
			}
			body, err = pbAnyValue(v)
			return err
		case 6:
			kv, err := p.lenField(wt)
			if err != nil {
				return err
			}
			return pbKeyValue(kv, attrs)
		case 9:
			v, err := p.lenField(wt)
			trace = v
			return err
		case 10:
			v, err := p.lenField(wt)
			span = v
			return err
		case 12:
			v, err := p.lenField(wt)
			event = string(v)
			return err
		}
		return errSkip
	})
	return res.record(scope, ts, observed, sevText, sevNum, body, attrs, trace, span, event), err
}

func pbKeyValue(b []byte, into map[string]string) error {
	var k, v string
	err := each(b, func(p *pbuf, num, wt int) error {
		switch num {
		case 1:
			s, err := p.lenField(wt)
			k = string(s)
			return err
		case 2:
			s, err := p.lenField(wt)
			if err != nil {
				return err
			}
			v, err = pbAnyValue(s)
			return err
		}
		return errSkip
	})
	if err == nil && k != "" {
		into[k] = v
	}
	return err
}

// pbAnyValue is an AnyValue as text: a string as it is, anything else as
// the JSON its value maps to.
func pbAnyValue(b []byte) (string, error) {
	v, err := pbAny(b)
	if err != nil {
		return "", err
	}
	return anyText(v), nil
}

func anyText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	j, _ := json.Marshal(v)
	return string(j)
}

func pbAny(b []byte) (any, error) {
	var out any
	err := each(b, func(p *pbuf, num, wt int) error {
		switch num {
		case 1:
			s, err := p.lenField(wt)
			out = string(s)
			return err
		case 2:
			v, err := p.varField(wt)
			out = v != 0
			return err
		case 3:
			v, err := p.varField(wt)
			out = int64(v) //nolint:gosec // int64 on the wire
			return err
		case 4:
			if err := want(wt, 1); err != nil {
				return err
			}
			v, err := p.fixed64()
			out = math.Float64frombits(v)
			return err
		case 5:
			ab, err := p.lenField(wt)
			if err != nil {
				return err
			}
			arr := []any{}
			err = each(ab, func(p *pbuf, num, wt int) error {
				if num != 1 {
					return errSkip
				}
				vb, err := p.lenField(wt)
				if err != nil {
					return err
				}
				v, err := pbAny(vb)
				arr = append(arr, v)
				return err
			})
			out = arr
			return err
		case 6:
			lb, err := p.lenField(wt)
			if err != nil {
				return err
			}
			m := map[string]any{}
			err = each(lb, func(p *pbuf, num, wt int) error {
				if num != 1 {
					return errSkip
				}
				kvb, err := p.lenField(wt)
				if err != nil {
					return err
				}
				var k string
				var v any
				err = each(kvb, func(p *pbuf, num, wt int) error {
					switch num {
					case 1:
						s, err := p.lenField(wt)
						k = string(s)
						return err
					case 2:
						s, err := p.lenField(wt)
						if err != nil {
							return err
						}
						v, err = pbAny(s)
						return err
					}
					return errSkip
				})
				m[k] = v
				return err
			})
			out = m
			return err
		case 7:
			s, err := p.lenField(wt)
			out = base64.StdEncoding.EncodeToString(s)
			return err
		}
		return errSkip
	})
	return out, err
}

// The OTLP JSON mapping: lowerCamelCase names, 64-bit integers as strings
// (or numbers), trace and span ids as hex.

type jsonKV struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type jsonLogs struct {
	ResourceLogs []struct {
		Resource struct {
			Attributes []jsonKV `json:"attributes"`
		} `json:"resource"`
		ScopeLogs []struct {
			Scope struct {
				Name string `json:"name"`
			} `json:"scope"`
			LogRecords []struct {
				TimeUnixNano         json.RawMessage `json:"timeUnixNano"`
				ObservedTimeUnixNano json.RawMessage `json:"observedTimeUnixNano"`
				SeverityNumber       json.RawMessage `json:"severityNumber"`
				SeverityText         string          `json:"severityText"`
				Body                 json.RawMessage `json:"body"`
				Attributes           []jsonKV        `json:"attributes"`
				TraceID              string          `json:"traceId"`
				SpanID               string          `json:"spanId"`
				EventName            string          `json:"eventName"`
			} `json:"logRecords"`
		} `json:"scopeLogs"`
	} `json:"resourceLogs"`
}

// DecodeOTLPJSON reads an ExportLogsServiceRequest in OTLP's JSON mapping.
func DecodeOTLPJSON(b []byte) ([]Record, error) {
	var req jsonLogs
	if err := json.Unmarshal(b, &req); err != nil {
		return nil, err
	}
	var out []Record
	for _, rl := range req.ResourceLogs {
		attrs, err := jsonAttrs(rl.Resource.Attributes)
		if err != nil {
			return nil, err
		}
		res := newResource(attrs)
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				ts, err1 := jsonInt(lr.TimeUnixNano)
				obs, err2 := jsonInt(lr.ObservedTimeUnixNano)
				sev, err3 := jsonInt(lr.SeverityNumber)
				la, err4 := jsonAttrs(lr.Attributes)
				body, err5 := jsonAnyText(lr.Body)
				if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
					return nil, err
				}
				trace, err6 := hex.DecodeString(lr.TraceID)
				span, err7 := hex.DecodeString(lr.SpanID)
				if err := errors.Join(err6, err7); err != nil {
					return nil, fmt.Errorf("traceId or spanId is not hex: %w", err)
				}
				out = append(out, res.record(sl.Scope.Name, ts, obs, lr.SeverityText, sev, body, la, trace, span, lr.EventName))
			}
		}
	}
	return out, nil
}

func jsonInt(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	s := strings.Trim(string(raw), `"`)
	if s == "" {
		return 0, nil
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, nil
	}
	// A severity number may come as its enum name.
	if strings.HasPrefix(s, "SEVERITY_NUMBER_") {
		return 0, nil
	}
	return 0, fmt.Errorf("%s is not an integer", raw)
}

func jsonAttrs(kvs []jsonKV) (map[string]string, error) {
	m := map[string]string{}
	for _, kv := range kvs {
		v, err := jsonAnyText(kv.Value)
		if err != nil {
			return nil, err
		}
		m[kv.Key] = v
	}
	return m, nil
}

func jsonAnyText(raw json.RawMessage) (string, error) {
	v, err := jsonAny(raw)
	if err != nil {
		return "", err
	}
	return anyText(v), nil
}

func jsonAny(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var v struct {
		StringValue *string                             `json:"stringValue"`
		BoolValue   *bool                               `json:"boolValue"`
		IntValue    json.RawMessage                     `json:"intValue"`
		DoubleValue *float64                            `json:"doubleValue"`
		BytesValue  *string                             `json:"bytesValue"`
		ArrayValue  *struct{ Values []json.RawMessage } `json:"arrayValue"`
		KvlistValue *struct{ Values []jsonKV }          `json:"kvlistValue"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	switch {
	case v.StringValue != nil:
		return *v.StringValue, nil
	case v.BoolValue != nil:
		return *v.BoolValue, nil
	case len(v.IntValue) > 0:
		return jsonInt(v.IntValue)
	case v.DoubleValue != nil:
		return *v.DoubleValue, nil
	case v.BytesValue != nil:
		return *v.BytesValue, nil
	case v.ArrayValue != nil:
		arr := []any{}
		for _, e := range v.ArrayValue.Values {
			x, err := jsonAny(e)
			if err != nil {
				return nil, err
			}
			arr = append(arr, x)
		}
		return arr, nil
	case v.KvlistValue != nil:
		m := map[string]any{}
		for _, kv := range v.KvlistValue.Values {
			x, err := jsonAny(kv.Value)
			if err != nil {
				return nil, err
			}
			m[kv.Key] = x
		}
		return m, nil
	}
	return nil, nil
}
