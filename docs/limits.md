# Limits

What a producer sends and what a reader or an archive asks for are bounded
by the limits below. They are policy, not protocol: none is a wire format
or a signed bound ([frozen.md](frozen.md)), and a later version may change
any of them without a new record version. The record bounds every reader
accepts ([spec.md](spec.md) section 11) are unchanged and sit above them.

A **hard cap** is refused before anything is sent: exit 2, with a message
naming the setting, the limit and why, for example

```text
blogs: -max-lines 70000 is over the limit of 65536: a reader decodes and filters every line of a batch before it prints one (docs/limits.md)
```

A **recommendation** passed is a `warning: ...` line on stderr, and the
command goes on.

## The table

| Limit | Default | Hard cap | Why | Plane and unicast |
| --- | --- | --- | --- | --- |
| Batch content, full-bytes `lines` (`-max-bytes`) | 49152 (plane), 262144 (unicast) | 262144 | The cap is the carried content bound every reader accepts; a larger batch would be split anyway. The plane default keeps a carrier at 49 packets | Plane warns above 65536 (64 packets); unicast defaults to the cap |
| Batch content, `gzip` (`-max-bytes`, bytes of lines before compression) | 262144 (plane), 1048576 (unicast) | 16777216 | The cap is the decoded bound every reader accepts. Log lines compress several times over, so the carried bytes stay near the full-bytes default | A plane carrier over 64 packets warns when it is built |
| Batch content, `digest` (`-max-bytes`) | 1048576 | 16777216 | The located bound every reader accepts; the content travels out of band, not on the plane | Same in both |
| Carrier size on the plane | warn over 72192 bytes (64 packets) | `object_bound` | Each packet carries 1128 bytes of object; every lost one is repaired, and the object must be whole within the fixed 10 s reassembly window | Plane only; once per run |
| `object_bound` | 1048576 | 8388608 | BRC-149: 1 MiB on the open path, 8 MiB on an authenticated one; no plane admits more | Plane warns above 1 MiB |
| Lines per batch (`-max-lines`) | 10000 | 65536 | A reader decodes and filters every line of a batch before it prints one | Same |
| Batch wait (`-max-wait`) | `1s` | `10ms` to `1h` | Below 10 ms every line is a batch (about 50 sat each); above an hour a line waits too long to be published. Warns when longer than `-interval` | Same |
| Publish rate (`-max-rate`, batches a second per stream) | 2 | 50 | Every subscribed host receives every batch, and a subscriber's delivery budget (hundreds of objects a second) is shared by all its topics; each batch costs about 50 sat and adds outputs a host keeps in memory | Unicast sends rate times hosts objects a second |
| Anchor interval (`-interval`) | `10m` | `1m` to `24h` | One anchor is in flight at a time and each waits for its block, so a shorter interval anchors no sooner; a batch is unproven until its anchor mines | Same |
| Batches per anchor (`-anchor-batches`) | 4096 | 65536 | Anchors early at this count. An audit asks one question per batch of the interval, and the producer's state rewrites every unanchored commitment on each batch; the cap is the signed per-interval bound | Same |
| Batch funding tree (`-tree-count`) | 100 | 1000 | Every carrier's BEEF carries its whole tree, about 49 bytes an output | Plane warns above 250 (about 11 packets of tree alone) |
| Hosts (`hosts`) | none | 16 | A reader or an auditor asks every host for every seq it needs; a unicast producer uploads every object to each | Unicast warns above 5: past that the plane is what removes the cost |
| Quorum (`quorum`) | `all` | the host count | A quorum no host set can meet would refuse every publish | Unicast only; warns below a majority. A sweep always needs every host named |
| Host submissions in flight | one object per stream, sent to all hosts at once | 16 (the host cap) | One carrier is published before the next is built, so a producer holds at most one request per host | Plane: one request, to the facade (a sweep also to each other host) |
| Tries per submission | 4, waits 1s, 2s, 4s | fixed | A host that fails four tries is reported missed; the carrier stays persisted | Plane: the facade; unicast: each host on its own |
| Request timeout (`timeout`) | `15s` | `1s` to `5m` | Bounds every request, so one host can hold up an object for at most four timeouts and 7 s | Same |
| Streams per home | none | 64 | One `produce` runs per home at a time (the coin pool is shared and locked), and `doctor` and `stream list` read every stream's state | Same; give another stream its own home |
| Archive recheck (`-recheck`, stored seqs asked about again per pass) | 1024 | 65536; `-recheck-all` for every one | Each is a lookup; asking about every stored seq on every pass does not scale. The pass continues after the last seq it asked about, oldest first | Same |
| Seqs per read | until no host holds the next | 65536 | Each seq is a lookup and a proof check; a longer read stops and names the `-from` to continue with | Same |
| Backfill range (`-seqs`) | none | 65536 | One interval's worth of positions per run; `-interval K` is bounded by the interval | Same |
| Lookup answer | 8 candidates per seq, 8 tokens per interval, 1 output for a funding tree | 8 (a reader keeps at most 8); a tree is one output | Bounds a host's storage, its answers, and a reader's root search. Stated in the specification, and listed as not frozen: a later version may raise it | Same |
| Digest-only content (`-content-dir`) | kept until removed | 16777216 per file | The located bound; warns when the directory's file system has under 1 GiB free, since every batch's content is written there | Content never rides the plane |

## Where the numbers come from

**The plane's object size.** An object larger than one packet is carried as
fragments at the fabric's MTU floor of 1280 bytes: less the IPv6 (40), UDP
(8) and fragment (104) headers, 1128 bytes of object each. Each lost
fragment is repaired by NACK, but the whole object must be reassembled
within a fixed 10 s, and a fragment that needs a second or third repair
round at a long round trip lands after the object was given up. In a
measurement on a test fabric at 1 % loss, no object of 1 or 8 packets was
lost and about 1 % of 16-packet objects were; across fabrics with a 141 ms
round trip, 12 % of 64-packet objects were lost. The plane default therefore
aims below 64 packets. Measured carriers, with a 100-output funding tree and
a proof:

| Content | Carrier BEEF | Packets |
| --- | --- | --- |
| 48 KiB | 54,727 bytes | 49 |
| 64 KiB | 71,119 bytes | 64 |
| 256 KiB | 267,729 bytes | 238 |

About 5.6 KB of each carrier is the tree, its proof and the carrier's own
fields; each tree output adds about 49 bytes.

**The plane's rate limits.** A plane bounds what one source submits and
what one subscriber receives, and the subscriber's budget is shared by every
topic it takes. A stream at the cap of 50 batches a second is a tenth of a
subscriber budget of 500 objects a second; the default of 2 leaves room for
many streams.

**Unicast.** No fragments: a host takes each object in one HTTP request.
The reference host accepts a submitted body of up to 64 MiB and a lookup
question of up to 64 KiB, far above any carrier the record bounds allow, so
a unicast producer can use the record's full 256 KiB. What grows instead is
the producer's upload and its retries, multiplied by the host count.

**Hosts' memory.** A host's index is in memory, and a restart restores it
by reading every unspent output of its topics. Each batch leaves at least
its carrier and the funding output it spends in a host's storage. A
reference host with its default heap has been seen to fail to start at
about 1.4 million outputs; at one batch a second a stream reaches that in
a little over a week. The rate cap bounds the growth; a host's retention
policy bounds the total.

**Cost.** At the default fee of 1 satoshi per byte a batch costs about
50 satoshis and an anchor about 600. At the default rate, a stream that
publishes every second it can spends at most 8,640,000 satoshis a day on
batches; at the cap, 216,000,000. Anchors cost at most one per block, about
86,400 satoshis a day.

**Archives.** Asking the host again about every stored seq on every pass
costs one lookup per seq per pass: a million-seq archive polling every
30 s would ask 33,000 questions a second. The recheck budget makes the
cost per pass fixed; a retraction is then found within `S / -recheck`
passes for S stored seqs. A spend read is made only for a carrier the host
no longer serves.

## Would need a frozen change

Nothing here needed one. These would, and are not made:

- **A lookup class for sweeps.** A question that answers "the sweep that
  spent this carrier's funding output" would let any backfill carry sweeps
  without a spend source. The question classes are signed
  ([frozen.md](frozen.md) section 6), so it would be a new, separately
  named class, added as the tree class was (a funding tree by its txid,
  which serves audits but names no spender).
- **A smaller carried-content bound for the plane.** The 256 KiB floor is
  what every reader accepts, and a floor is never lowered. The plane default
  is producer policy for that reason.
- **A published object bound.** A producer learns the plane's object bound
  from its operator (`object_bound`); nothing on the wire advertises it.
