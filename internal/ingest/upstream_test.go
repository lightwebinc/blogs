package ingest

import (
	"encoding/binary"
	"math"
	"testing"
)

func pbFixed32(b []byte, num int, v uint32) []byte {
	return binary.LittleEndian.AppendUint32(pbTag(b, num, 5), v)
}

// Every field of the upstream definitions the doors read, by number and
// wire type, as opentelemetry-proto v1.11.1 (logs/v1/logs.proto,
// common/v1/common.proto, resource/v1/resource.proto,
// collector/logs/v1/logs_service.proto) declares them: the fields mapped
// are read, every other declared field is skipped by its wire type, and a
// mapped field arriving with another wire type is malformed.
func TestOTLPUpstreamFields(t *testing.T) {
	kv := func(k string, v []byte) []byte { return pbBytes(pbStr(nil, 1, k), 2, v) }
	// Resource: attributes 1, dropped_attributes_count 2, entity_refs 3.
	entity := pbStr(pbStr(nil, 1, "https://schema"), 2, "service")
	entity = pbStr(pbStr(entity, 3, "service.name"), 4, "service.version")
	resource := pbBytes(nil, 1, kv("service.name", pbStr(nil, 1, "api")))
	resource = pbVarint(resource, 2, 3)
	resource = pbBytes(resource, 3, entity)

	// InstrumentationScope: name 1, version 2, attributes 3,
	// dropped_attributes_count 4.
	scope := pbStr(nil, 1, "lib")
	scope = pbStr(scope, 2, "1.2.3")
	scope = pbBytes(scope, 3, kv("scope.attr", pbStr(nil, 1, "x")))
	scope = pbVarint(scope, 4, 1)

	// LogRecord: every field 1 to 12.
	lr := pbFixed64(nil, 1, 1791400000000000001)
	lr = pbVarint(lr, 2, 13)
	lr = pbStr(lr, 3, "")
	lr = pbBytes(lr, 5, pbStr(nil, 1, "hello"))
	// AnyValue 1 to 8: string, bool, int (negative, ten varint bytes),
	// double, array, kvlist, bytes, and string_value_strindex, which only
	// the profiles signal defines and a logs reader treats as absent.
	lr = pbBytes(lr, 6, kv("s", pbStr(nil, 1, "v")))
	lr = pbBytes(lr, 6, kv("b", pbVarint(nil, 2, 1)))
	neg := int64(-5)
	lr = pbBytes(lr, 6, kv("i", pbVarint(nil, 3, uint64(neg))))
	lr = pbBytes(lr, 6, kv("d", pbFixed64(nil, 4, math.Float64bits(0.25))))
	arr := pbBytes(pbBytes(nil, 1, pbStr(nil, 1, "a")), 1, pbVarint(nil, 3, 2))
	lr = pbBytes(lr, 6, kv("a", pbBytes(nil, 5, arr)))
	kvl := pbBytes(nil, 1, kv("k", pbVarint(nil, 2, 0)))
	lr = pbBytes(lr, 6, kv("m", pbBytes(nil, 6, kvl)))
	lr = pbBytes(lr, 6, kv("y", pbBytes(nil, 7, []byte{0xff, 0x00})))
	lr = pbBytes(lr, 6, kv("strindex", pbVarint(nil, 8, 4)))
	// KeyValue key_strindex 3 (profiles only): a pair with no key is not
	// an attribute.
	lr = pbBytes(lr, 6, pbBytes(pbVarint(nil, 3, 1), 2, pbStr(nil, 1, "keyless")))
	lr = pbVarint(lr, 7, 2)        // dropped_attributes_count
	lr = pbFixed32(lr, 8, 1)       // flags
	lr = pbBytes(lr, 9, []byte{1}) // trace_id
	lr = pbBytes(lr, 10, []byte{2})
	lr = pbFixed64(lr, 11, 99) // observed_time_unix_nano
	lr = pbStr(lr, 12, "user.login")

	// ScopeLogs: scope 1, log_records 2, schema_url 3; ResourceLogs:
	// resource 1, scope_logs 2, schema_url 3; the request: resource_logs 1.
	sl := pbStr(pbBytes(pbBytes(nil, 1, scope), 2, lr), 3, "https://schema")
	rl := pbStr(pbBytes(pbBytes(nil, 1, resource), 2, sl), 3, "https://schema")
	req := pbBytes(nil, 1, rl)
	recs, err := DecodeOTLPProto(req)
	if err != nil || len(recs) != 1 {
		t.Fatalf("%v %d", err, len(recs))
	}
	r := recs[0]
	want := map[string]string{
		"scope": "lib", "s": "v", "b": "true", "i": "-5", "d": "0.25", "a": `["a",2]`, "m": `{"k":false}`, "y": "/wA=",
		"strindex": "", "trace_id": "01", "span_id": "02", "event_name": "user.login",
	}
	for k, v := range want {
		if got, ok := r.Attrs[k]; !ok || got != v {
			t.Fatalf("attribute %s: %q (present %v), want %q; all %v", k, got, ok, v, r.Attrs)
		}
	}
	if len(r.Attrs) != len(want) || r.Ts != 1791400000000000001 || r.Sev != "WARN" || r.Body != "hello" || r.Labels["service_name"] != "api" || len(r.Labels) != 1 {
		t.Fatalf("record %+v", r)
	}

	// A mapped field with another wire type is malformed.
	for name, bad := range map[string][]byte{
		"time_unix_nano as varint":       pbVarint(nil, 1, 1),
		"severity_number as bytes":       pbStr(nil, 2, "9"),
		"severity_text as varint":        pbVarint(nil, 3, 1),
		"body as varint":                 pbVarint(nil, 5, 1),
		"attributes as fixed64":          pbFixed64(nil, 6, 1),
		"trace_id as varint":             pbVarint(nil, 9, 1),
		"observed_time_unix_nano as i32": pbFixed32(nil, 11, 1),
		"event_name as varint":           pbVarint(nil, 12, 1),
		"string_value as varint":         pbBytes(nil, 5, pbVarint(nil, 1, 1)),
		"bool_value as bytes":            pbBytes(nil, 5, pbStr(nil, 2, "x")),
		"double_value as varint":         pbBytes(nil, 5, pbVarint(nil, 4, 1)),
	} {
		req := pbBytes(nil, 1, pbBytes(nil, 2, pbBytes(nil, 2, bad)))
		if _, err := DecodeOTLPProto(req); err == nil {
			t.Fatalf("%s decoded", name)
		}
	}
	if _, err := DecodeOTLPProto(pbVarint(nil, 1, 1)); err == nil {
		t.Fatal("resource_logs as varint decoded")
	}

	// The JSON mapping's eventName.
	js := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1","eventName":"user.login","body":{"stringValue":"x"}}]}]}]}`
	recs, err = DecodeOTLPJSON([]byte(js))
	if err != nil || len(recs) != 1 || recs[0].Attrs["event_name"] != "user.login" {
		t.Fatalf("json eventName: %v %+v", err, recs)
	}
}

// Every field of Loki's push.proto (pkg/push/push.proto, v3.7.9) and the
// google.protobuf.Timestamp it carries: PushRequest streams 1 and format 2;
// StreamAdapter labels 1, entries 2, hash 3; EntryAdapter timestamp 1,
// line 2, structuredMetadata 3, parsed 4; LabelPairAdapter name 1, value 2.
func TestLokiUpstreamFields(t *testing.T) {
	pair := func(k, v string) []byte { return pbStr(pbStr(nil, 1, k), 2, v) }
	ts := pbVarint(pbVarint(nil, 1, 1791400000), 2, 5)
	entry := pbBytes(nil, 1, ts)
	entry = pbStr(entry, 2, "a line")
	entry = pbBytes(entry, 3, pbVarint(pair("trace_id", "cd"), 9, 1)) // an unknown field in a pair is skipped
	entry = pbBytes(entry, 4, pair("parsed", "querier only"))         // parsed: filled by a querier, not a push
	stream := pbStr(nil, 1, `{service_name="api"}`)
	stream = pbBytes(stream, 2, entry)
	stream = pbVarint(stream, 3, 12345) // hash
	req := pbStr(pbBytes(nil, 1, stream), 2, "loki")
	recs, err := DecodeLokiProto(req)
	if err != nil || len(recs) != 1 {
		t.Fatalf("%v %d", err, len(recs))
	}
	r := recs[0]
	if r.Ts != 1791400000000000005 || r.Body != "a line" || len(r.Attrs) != 1 || r.Attrs["trace_id"] != "cd" || r.Labels["service_name"] != "api" {
		t.Fatalf("record %+v", r)
	}
	for name, bad := range map[string][]byte{
		"streams as varint":   pbVarint(nil, 1, 1),
		"labels as varint":    pbBytes(nil, 1, pbVarint(nil, 1, 1)),
		"entries as fixed64":  pbBytes(nil, 1, pbFixed64(nil, 2, 1)),
		"timestamp as varint": pbBytes(nil, 1, pbBytes(nil, 2, pbVarint(nil, 1, 1))),
		"seconds as bytes":    pbBytes(nil, 1, pbBytes(nil, 2, pbBytes(nil, 1, pbStr(nil, 1, "1")))),
		"line as varint":      pbBytes(nil, 1, pbBytes(nil, 2, pbVarint(nil, 2, 1))),
		"pair name as varint": pbBytes(nil, 1, pbBytes(nil, 2, pbBytes(nil, 3, pbVarint(nil, 1, 1)))),
		"negative nanos":      pbBytes(nil, 1, pbBytes(nil, 2, pbBytes(nil, 1, pbVarint(nil, 2, uint64(math.MaxUint64))))),
	} {
		if _, err := DecodeLokiProto(bad); err == nil {
			t.Fatalf("%s decoded", name)
		}
	}
}
