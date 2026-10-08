# blogs as a logging backend

blogs can sit behind the log shippers and the front end a team already
runs. Two commands do it:

- `blogs ingest`, the **provenance sidecar**: syslog, OTLP logs and Loki
  push in; each record published as a line of a signed batch on a blogs
  stream; each published line forwarded, unchanged, to a stock Loki with
  where it was published.
- `blogs gateway`, the **read gateway**: the Loki HTTP API in front of that
  Loki, for Grafana and any other Loki client, marking every line it
  returns `verified` or `unverified` against the interval roots an archive
  holds.

Nothing here changes the blogs record or [frozen.md](frozen.md): a line is
content, and everything below is a convention of these two commands.

```text
 shippers                         sidecar (blogs ingest)                hosts            archives
 ────────                         ──────────────────────                ─────            ────────
 rsyslog (TCP, TLS) ─────────────▶ syslog doors   ─┐                ┌─▶ host A ──────▶ archive A ─┐
 OTel Collector (OTLP/HTTP) ─────▶ /v1/logs        ├─▶ line form ─▶ │   host B ──────▶ archive B  │ roots agree
 Alloy, Promtail, Fluent Bit ────▶ /loki/api/v1/push┘  batches ──────┴─▶ host C ──────▶ archive C ─┘
                                        │      (signed, anchored each interval)
                                        └─ each published line + placement ─▶ Loki (stock, unmodified)
                                                                                 │
 Grafana (unchanged) ◀── Loki API, each line marked ── blogs gateway ◀───────────┘
                                                            └─ checks against archive A's interval files
```

## The line form (version 1)

Every record, whatever door it came in by, becomes one line: a JSON object
with its keys in order and no spaces, so the same record is the same bytes
wherever it is built.

```json
{"attrs":{"trace_id":"0af7651916cd43dd8448eb211c80319c"},"body":"GET /health 200","labels":{"env":"prod","service_name":"api"},"sev":"INFO","ts":"1791400000123456789","v":1}
```

| Key | Holds |
| --- | --- |
| `attrs` | everything that is not a label: OTLP attributes, Loki structured metadata, syslog fields; omitted when empty |
| `body` | the message |
| `labels` | the low-cardinality keys the store indexes; Loki's stream labels |
| `sev` | the severity as text; omitted when unknown |
| `ts` | the record's own time, nanoseconds since the epoch, as a string: a claim of the shipper, not proof of time |
| `v` | the form's version, 1 |

A record that encodes to more than 65,535 bytes is refused (HTTP 400),
because the producer would carry it as several lines.

## The doors

| Flag | Protocol | Mapping |
| --- | --- | --- |
| `-syslog ADDR` | syslog over TCP, RFC 6587 framing (octet counted or newline, decided per frame) | RFC 5424: `APP-NAME` is the label `service_name`, `HOSTNAME` the label `host`; facility, `PROCID`, `MSGID` and each structured data parameter (`sd.<id>.<name>`) are attributes; severity by name (`err`, `info` ...). A message that is not RFC 5424 keeps its priority and is the body whole |
| `-syslog-tls ADDR` | the same over TLS (RFC 5425); `-tls-client-ca` requires client certificates | as above |
| `-http ADDR` | OTLP logs over HTTP, `POST /v1/logs`, protobuf or JSON | the resource's `service.name` is `service_name`; other resource attributes are `resource.<key>`; record attributes as they are; `trace_id`, `span_id` in hex; `scope`; severity text, or the name of the severity number |
| `-http ADDR` | Loki push, `POST /loki/api/v1/push`, protobuf (snappy) or JSON | stream labels are labels; structured metadata are attributes; the line is the body |

The HTTP doors also serve `GET /metrics` and `GET /healthz`. A gzip
`Content-Encoding` is accepted; a body is bounded at 16 MiB before and after
decompression.

**Acknowledgment.** An HTTP request is answered (200 for OTLP, 204 for Loki
push) once every one of its records is in a batch that is persisted and
published. A request that cannot be taken now is answered 503 with
`Retry-After`; shippers retry it. Delivery is at least once: a record whose
request failed after its batch was persisted may be published twice when
the sender retries. Syslog has no acknowledgment: its records are queued,
and a full queue holds the connection back.

**Batches.** Lines are cut into batches by `-max-bytes`, `-max-lines` and
`-max-wait`, at most `-max-rate` batches a second per stream, and each
interval (`-interval`) is anchored, exactly as `blogs produce` does
([limits.md](limits.md)). The fee policy is the home's (default 0.1 sat a
byte, [configuration.md](configuration.md)).

## Confidential records

A record carrying `-confidential-label` (default `confidential=true`) goes
to `-confidential-stream`, which is digest-only: the stream carries each
batch's SHA-256 and length, and the lines are written to `-content-dir` and
nowhere else ([spec.md](spec.md), the digest-only profile). Hosts and
archives without the content hold the digest and can prove the batch's
place; an archive or gateway given `-content-dir` (or an allowed fetch hint,
`-url`) can check the lines themselves. The lines are still forwarded to
Loki, which is the deployment's own index.

## Forwarding to Loki

With `-loki`, every published line is pushed to Loki's push API, in the
order published, unchanged, under its labels, with its placement as
structured metadata:

| Name | Value |
| --- | --- |
| `blogs_stream` | the stream |
| `blogs_producer` | the producer key, hex |
| `blogs_seq` | the batch's seq |
| `blogs_line` | the line's position in the batch, from 1 |

Loki must accept structured metadata (schema v13, `allow_structured_metadata`,
the default in Loki 3). A push that fails is retried for about five
minutes; one that still fails, or that finds the queue full, is counted
(`blogs_forward_dropped_lines_total`) and dropped from Loki only: the lines
stay on the stream, where every archive holds them. Loki runs as a separate
service, unmodified.

## The read gateway

```bash
blogs gateway -archive /var/lib/blogs/archive -loki http://127.0.0.1:3100 -listen 127.0.0.1:3101 \
  -content-dir /var/lib/blogs/content
```

Point Grafana's Loki data source at the gateway. Every request is passed to
Loki. The lines of a `query` or `query_range` answer are each checked:

1. its placement names a stream in the archive, and an interval file whose
   inclusion paths lead to its root covers its seq;
2. the batch at that seq is the carrier the root commits, and still passes
   the carrier rules (producer key, signature, seq);
3. its content, read from the carrier or, for a digest-only batch, from
   `-content-dir` and checked against the digest, holds this exact line at
   this position;
4. the line's `ts` and labels agree with what Loki answered.

The mark is `blogs_state`, `verified` or `unverified`, and for an
unverified line `blogs_reason`:

| Reason | Meaning |
| --- | --- |
| `pending` | no interval the archive has proven covers the line yet: normal for the interval plus a block plus an archive pass after it was sent |
| `altered` | the line is not the one at its position in the committed batch |
| `relabeled` | the text is, but its time or a label in the store differ |
| `retracted` | the producer retracted the batch; the archive holds the receipt, and the anchor still commits the position |
| `hole` | the root commits the position and the archive never held the batch |
| `no-provenance` | the line carries no placement |
| `unreadable` | the archive's files for the batch did not read or verify |

A client that asks for categorized labels (`X-Loki-Response-Encoding-Flags:
categorize-labels`, which Grafana sends) gets the marks as structured
metadata on each line, where Grafana shows them as fields; any other client
gets them as stream labels. Either way the marks render without a plugin: in
a logs panel's details, and in a table panel after an "Extract fields"
transformation of the labels, where a value mapping can color
`verified` and `unverified`.

`GET /blogs/v1/coverage` answers, per stream in the archive, every interval
proven (first and last seq, root, token, height), the holes, and each
retraction receipt with the sweep that retracted it.

What the gateway does not prove: that a search found every matching line
(a store can omit a line and return only true ones; coverage shows what the
archive holds, not what the store searched), and anything on
`/loki/api/v1/tail`, which is passed through unmarked. An auditor who trusts
neither the gateway nor its archive runs `blogs verify-interval` against any
host or archive ([usage.md](usage.md)).

## Metering

Counters a billing pipeline scrapes; nothing here prices or bills.

| Metric | Where | Unit |
| --- | --- | --- |
| `blogs_ingest_bytes_total{stream}` | sidecar | bytes of lines published, newline included: **GB ingested** |
| `blogs_ingest_lines_total{stream}`, `blogs_ingest_batches_total{stream}` | sidecar | lines, batches |
| `blogs_ingest_received_bytes_total{door}` | sidecar | bytes on the wire, before decompression |
| `blogs_ingest_records_total{door}`, `blogs_ingest_requests_total{door,outcome}` | sidecar | records, requests and syslog connections |
| `blogs_forward_lines_total`, `blogs_forward_dropped_lines_total` | sidecar | lines pushed to Loki, and not |
| `blogs_retained_bytes{stream}` | gateway | bytes the archive keeps now (batches and content) |
| `blogs_retained_byte_seconds_total{stream}` | gateway | retained bytes integrated over time, measured every `-meter`: divide by 2.592e15 for **GB-months** (30-day months) |
| `blogs_gateway_lines_total{state,reason}` | gateway | lines returned, by mark |

## Running it

One sidecar process drives both streams of one producer home and holds the
home's lock, so `blogs sweep` (retraction) runs with the sidecar stopped.

```bash
blogs stream new app                 # app_<suffix>
blogs stream new sec                 # sec_<suffix>, the confidential stream
blogs ingest -stream app_<suffix> -confidential-stream sec_<suffix> -content-dir /var/lib/blogs/content \
  -http 127.0.0.1:4318 -syslog 127.0.0.1:6514 -syslog-tls 0.0.0.0:6515 \
  -tls-cert server.pem -tls-key server.key -tls-client-ca ca.pem \
  -loki http://127.0.0.1:3100
for s in app_<suffix> sec_<suffix>; do   # one archive per host; three hosts, three archives
  blogs archive $s <producer> -dir /var/lib/blogs/archive -host https://host-a.example.com -follow \
    -content-dir /var/lib/blogs/content &
done
blogs gateway -archive /var/lib/blogs/archive -loki http://127.0.0.1:3100 -content-dir /var/lib/blogs/content
```

Shipper settings, in short:

| Shipper | Setting |
| --- | --- |
| rsyslog | `omfwd`, `protocol="tcp"`, `TCP_Framing="octet-counted"`, `template="RSYSLOG_SyslogProtocol23Format"`; for TLS add `StreamDriver="gtls" StreamDriverMode="1" StreamDriverAuthMode="x509/name"` and the client certificate |
| OpenTelemetry Collector | exporter `otlphttp` with `logs_endpoint: http://<sidecar>/v1/logs` |
| Grafana Alloy, Promtail, Fluent Bit, Vector | Loki output with URL `http://<sidecar>/loki/api/v1/push` |

## Exit test

`scripts/backend-exit-test.py` checks a running deployment (standard
library Python only):

```bash
A="--archive /a --archive /b --archive /c --stream app_<suffix> --stream sec_<suffix> --producer <key> \
   --gateway http://127.0.0.1:3101 --loki http://127.0.0.1:3100"
scripts/backend-exit-test.py roots   $A    # the three archives agree on every interval they share
scripts/backend-exit-test.py tamper  $A    # an altered copy pushed into Loki is marked altered
scripts/backend-exit-test.py retract $A --blogs blogs   # with the sidecar stopped: a receipted hole
scripts/backend-exit-test.py soak    $A --duration 604800 --every 300   # the 7-day run
```

Each prints one JSON line per check and exits 0 only when it passed.
