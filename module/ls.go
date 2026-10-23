package module

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// MaxCandidates is how many candidates are kept and answered per seq, and
// anchor tokens per interval (spec section 11).
const MaxCandidates = 8

// The statuses of spec sections 8.2 and 8.3.
const (
	BatchRetracted = "retracted"
	BatchFork      = "fork"
	BatchGap       = "gap"
	BatchLinked    = "linked"

	TokenCanonical = "canonical"
	TokenBranch    = "anchor-branch"
	TokenJoin      = "anchor-join"
	TokenPending   = "pending"

	IntervalComplete     = "complete"
	IntervalIncomplete   = "incomplete"
	IntervalRootMismatch = "root-mismatch"
)

// noPosition is a token whose proof is unknown: it sorts after every mined
// one.
const noPosition = math.MaxUint64

type batchEntry struct {
	seq uint64
	// prev is the previous batch's commitment, hash byte order, hex.
	prev string
	// c is the commitment, hash byte order; cKey its hex, the candidate order.
	c    [32]byte
	cKey string
	// txid is display hex.
	txid string
	vout uint32
	// funding is the funding outpoint the carrier spends (opKey).
	funding string
}

type tokenEntry struct {
	anchor *logrec.Anchor
	txid   string
	// txidKey is the txid in hash byte order, hex: the order among tokens
	// of one interval.
	txidKey  string
	vout     uint32
	outpoint string
	// height and index are the block height and position in the block;
	// noPosition when the proof is unknown.
	height, index uint64
	// inputs are the outpoints the token's transaction spends.
	inputs []string
}

type chainCache struct {
	tokens     []*tokenEntry
	joinFailed map[string]bool
}

type stream struct {
	producer, stream string
	batches          map[uint64][]*batchEntry
	tokens           map[uint64][]*tokenEntry
	highest          uint64
	// retiredInterval, retiredSeq and retiredC say what retention dropped:
	// intervals 1 to retiredInterval, seqs 1 to retiredSeq; retiredC is C at
	// retiredSeq (hex).
	retiredInterval, retiredSeq uint64
	retiredC                    string
	chain                       *chainCache
}

// IntervalReport is one interval of a canonical chain.
type IntervalReport struct {
	Interval uint64
	First    uint64
	Count    uint64
	Txid     string
	Status   string
	// Missing are the seqs the interval covers that no batch and no receipt
	// holds.
	Missing []uint64
	// Retired is a complete interval whose carriers retention dropped.
	Retired bool
}

// TokenReport is a token outside the canonical chain, with why.
type TokenReport struct {
	Interval uint64
	Txid     string
	Status   string
}

// StreamReport is the statuses of one (producer, stream).
type StreamReport struct {
	Producer string
	Stream   string
	// Highest is the highest seq held.
	Highest uint64
	// Holes are the seqs from 1 to Highest with no batch and no receipt, as
	// inclusive ranges.
	Holes [][2]uint64
	// Forks are the seqs with the status fork.
	Forks []uint64
	// Retracted are the seqs whose every candidate is retracted.
	Retracted []uint64
	// Intervals are the canonical chain's, in order.
	Intervals []IntervalReport
	// Tokens are those outside the canonical chain.
	Tokens []TokenReport
}

// RetentionPass is what a retention pass did.
type RetentionPass struct {
	// Intervals is how many intervals had their carriers dropped.
	Intervals int
	// Carriers is how many carriers left the index.
	Carriers int
	// Deleted is how many carrier outputs were deleted from storage, this
	// pass's and any left from before.
	Deleted int
}

// Deleter is a storage that can delete an output: what retention asserts
// for on the restore seam, beyond the contract's RestoreStorage.
type Deleter interface {
	DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error
}

type ref struct {
	topic  string
	kind   string // batch, token, coin
	stream *stream
}

type drop struct {
	op    transaction.Outpoint
	topic string
}

// Service is ls_log: one lookup service for every stream a host carries
// (spec sections 7.2 and 8.2 to 8.5).
//
// It indexes what the topic managers admitted, per (producer, stream):
// batches by seq (at most 8 candidates per seq), anchor tokens by interval
// (at most 8 per interval), and, per outpoint, the transactions that spent
// it, read from each admitted transaction's own inputs. Every status is a
// function of that set, computed when asked, so arrival order and restart
// order never change it. It answers exactly the three free question
// classes and refuses every other query with an error. Statuses are
// reported by Report, the metrics and the log, never inside an answer.
type Service struct {
	topics    map[string]bool
	host      bmodule.Host
	retention *Retention

	mu sync.Mutex
	// streams: producer hex | stream -> index.
	streams map[string]*stream
	// byOutpoint: outpoint -> what was indexed there, for eviction; an
	// outpoint may be held on several topics.
	byOutpoint map[string][]ref
	// held: topic | outpoint -> the admitted output, for the topic
	// manager's previous coins.
	held map[string]logrec.Coin
	// trees: topic | txid -> the funding outputs the topic holds of it.
	trees map[string]map[uint32]bool
	// carriersByFunding: funding outpoint -> the carriers (display txid)
	// that spend it.
	carriersByFunding map[string]map[string]bool
	// spenders: outpoint -> txid -> whether that spender is mined.
	spenders map[string]map[string]bool
	// retracted: carrier txids retracted by a mined spend of their funding
	// outpoint.
	retracted map[string]bool
	noted     map[string]bool
	forkSeqs  int
	storage   bmodule.RestoreStorage
	// marks: producer | stream -> the retained prefix's mark.
	marks map[string]Mark
	// treeLow: topic | tree txid -> the lowest funding output whose carrier
	// retention dropped.
	treeLow map[string]uint32
	drops   []drop
	// stateDirty: the marks or treeLow changed since the state file was
	// last written.
	stateDirty bool
	warned     map[string]bool
	retainMu   sync.Mutex
}

var (
	_ bmodule.LookupService     = (*Service)(nil)
	_ bmodule.Restorer          = (*Service)(nil)
	_ bmodule.RetentionListener = (*Service)(nil)
)

// NewService makes ls_log for these topics. retention nil keeps everything.
func NewService(topics []string, host bmodule.Host, retention *Retention) (*Service, error) {
	s := &Service{topics: map[string]bool{}, host: host, retention: retention, marks: map[string]Mark{}, treeLow: map[string]uint32{}, warned: map[string]bool{}}
	for _, t := range topics {
		s.topics[t] = true
	}
	s.clear()
	if retention != nil {
		st, err := retention.Store.Load()
		if err != nil {
			return nil, err
		}
		for _, m := range st.Marks {
			s.marks[m.Producer+"|"+m.Stream] = m
		}
		for _, t := range st.Trees {
			s.treeLow[t.Key] = t.Vout
		}
	}
	return s, nil
}

func (s *Service) clear() {
	s.streams = map[string]*stream{}
	s.byOutpoint = map[string][]ref{}
	s.held = map[string]logrec.Coin{}
	s.trees = map[string]map[uint32]bool{}
	s.carriersByFunding = map[string]map[string]bool{}
	s.spenders = map[string]map[string]bool{}
	s.retracted = map[string]bool{}
	s.noted = map[string]bool{}
	s.forkSeqs = 0
	s.drops = nil
}

func opKey(txid string, vout uint32) string { return txid + "." + strconv.FormatUint(uint64(vout), 10) }

func splitOp(op string) (string, uint32) {
	dot := strings.LastIndexByte(op, '.')
	n, _ := strconv.ParseUint(op[dot+1:], 10, 32)
	return op[:dot], uint32(n)
}

func parseOp(op string) (transaction.Outpoint, bool) {
	txid, vout := splitOp(op)
	h, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return transaction.Outpoint{}, false
	}
	return transaction.Outpoint{Txid: *h, Index: vout}, true
}

// hashKey is a display txid in hash byte order, hex.
func hashKey(h *chainhash.Hash) string { return hex.EncodeToString(h[:]) }

func inputsOf(tx *transaction.Transaction) []string {
	var out []string
	for _, in := range tx.Inputs {
		if in.SourceTXID != nil {
			out = append(out, opKey(in.SourceTXID.String(), in.SourceTxOutIndex))
		}
	}
	return out
}

func position(tx *transaction.Transaction) (uint64, uint64) {
	p := tx.MerklePath
	if p == nil || len(p.Path) == 0 {
		return noPosition, noPosition
	}
	id := tx.TxID()
	for _, l := range p.Path[0] {
		if l.Hash != nil && l.Hash.IsEqual(id) {
			return uint64(p.BlockHeight), l.Offset
		}
	}
	return noPosition, noPosition
}

// pushes are a script's data pushes after its first two elements (a
// lock-before PushDrop's key and OP_CHECKSIG), up to the first non-push.
func pushes(s []byte) [][]byte {
	chunks, err := script.DecodeScript(s)
	if err != nil || len(chunks) < 3 {
		return nil
	}
	var out [][]byte
	for _, c := range chunks[2:] {
		if c.Op > script.OpPUSHDATA4 {
			break
		}
		if c.Data == nil {
			out = append(out, []byte{})
		} else {
			out = append(out, c.Data)
		}
	}
	return out
}

// Coin is the output the topic holds at an outpoint: from the engine's
// storage once restore has handed it over (the host calls restore before
// it accepts a submission), otherwise from what this service indexed.
func (s *Service) Coin(ctx context.Context, topic string, op transaction.Outpoint) (logrec.Coin, bool, error) {
	s.mu.Lock()
	storage := s.storage
	if storage == nil {
		c, ok := s.held[topic+"|"+opKey(op.Txid.String(), op.Index)]
		s.mu.Unlock()
		return c, ok, nil
	}
	s.mu.Unlock()
	o, err := storage.FindOutput(ctx, &op, topic)
	if err != nil || o == nil {
		return logrec.Coin{}, false, err
	}
	c := logrec.Coin{Satoshis: o.Satoshis, LockingScript: []byte{}}
	if o.OutputScript != nil {
		c.LockingScript = []byte(*o.OutputScript)
	}
	return c, true, nil
}

// ---- gauges

// Counts are the gauges' values.
type Counts struct {
	Streams, Batches, Tokens, Trees, Retracted, ForkSeqs int
	Missing, Retired                                     uint64
}

// Counts reads every gauge at once.
func (s *Service) Counts() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := Counts{Streams: len(s.streams), Trees: len(s.trees), Retracted: len(s.retracted), ForkSeqs: s.forkSeqs}
	for _, st := range s.streams {
		for _, l := range st.batches {
			c.Batches += len(l)
		}
		for _, l := range st.tokens {
			c.Tokens += len(l)
		}
		held := st.retiredSeq + uint64(len(st.batches))
		if st.highest > held {
			c.Missing += st.highest - held
		}
	}
	for _, m := range s.marks {
		c.Retired += m.Seq
	}
	return c
}

// ---- admission

// AdmissionMode implements module.LookupService: the whole transaction, as
// a carrier's funding outpoint and every spend are its inputs.
func (*Service) AdmissionMode() bmodule.AdmissionMode { return bmodule.AdmitWholeTx }

// SpendNotificationMode implements module.LookupService: spends are read
// from admitted transactions, never from the engine's notices.
func (*Service) SpendNotificationMode() bmodule.SpendNotificationMode { return bmodule.SpendNone }

// OutputAdmittedByTopic implements module.LookupService.
func (s *Service) OutputAdmittedByTopic(_ context.Context, p *bmodule.OutputAdmittedByTopic) error {
	if p == nil || p.Mode != bmodule.AdmitWholeTx || !s.topics[p.Topic] {
		return nil
	}
	_, tx, _, err := transaction.ParseBeef(p.AtomicBEEF)
	if err != nil || tx == nil {
		s.host.Log("ls_log ignored an admission whose BEEF did not parse", map[string]any{"topic": p.Topic})
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, err := s.index(p.Topic, tx, p.Outpoint.Index)
	if err != nil {
		s.host.Log("ls_log could not index an admission", map[string]any{"topic": p.Topic, "outpoint": p.Outpoint.String(), "err": err.Error()})
		return nil
	}
	if ok {
		s.noteSpends(tx)
	}
	return nil
}

// OutputEvicted implements module.LookupService.
func (s *Service) OutputEvicted(_ context.Context, op *transaction.Outpoint) error {
	if op == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropOutpoint(opKey(op.Txid.String(), op.Index), "")
	return nil
}

// OutputNoLongerRetainedInHistory implements module.RetentionListener.
func (s *Service) OutputNoLongerRetainedInHistory(_ context.Context, op *transaction.Outpoint, topic string) error {
	if op == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropOutpoint(opKey(op.Txid.String(), op.Index), topic)
	return nil
}

// index indexes one admitted output of tx on topic. False for a carrier at
// a position retention has dropped: it is not indexed, and its output is
// deleted from storage at the next pass.
func (s *Service) index(topic string, tx *transaction.Transaction, vout uint32) (bool, error) {
	if int(vout) >= len(tx.Outputs) {
		return true, nil
	}
	out := tx.Outputs[vout]
	var lock []byte
	if out.LockingScript != nil {
		lock = []byte(*out.LockingScript)
	}
	id := tx.TxID()
	txid := id.String()
	op := opKey(txid, vout)
	name := strings.TrimPrefix(topic, logrec.TopicPrefix)
	fields := pushes(lock)
	isPD := fields != nil && isPushDrop(lock)
	if isPD && logrec.IsBatch(fields[0]) {
		b, err := logrec.DecodeBatch(fields[0])
		if err != nil {
			return true, err
		}
		if b.Stream != name {
			return true, nil
		}
		ins := inputsOf(tx)
		if len(ins) == 0 {
			return true, nil
		}
		st := s.streamOf(hex.EncodeToString(b.Producer), b.Stream)
		if b.Seq <= st.retiredSeq {
			s.drops = append(s.drops, drop{op: transaction.Outpoint{Txid: *id, Index: vout}, topic: topic})
			return false, nil
		}
		c := [32]byte(*id)
		// A carrier output is never a valid anchor input; it is held with
		// an empty script so that rule 7 refuses an anchor that spends it.
		s.hold(topic, op, logrec.Coin{LockingScript: []byte{}, Satoshis: out.Satoshis})
		s.addBatch(topic, st, &batchEntry{seq: b.Seq, prev: hex.EncodeToString(b.Prev[:]), c: c, cKey: hex.EncodeToString(c[:]), txid: txid, vout: vout, funding: ins[0]})
		return true, nil
	}
	s.hold(topic, op, logrec.Coin{LockingScript: bytes.Clone(lock), Satoshis: out.Satoshis})
	if isPD && len(fields) == 3 && bytes.Equal(fields[0], logrec.TagAnchor) {
		a, err := logrec.DecodeAnchor(fields[1])
		if err != nil {
			return true, err
		}
		if a.Stream != name {
			return true, nil
		}
		st := s.streamOf(hex.EncodeToString(a.Producer), a.Stream)
		h, i := position(tx)
		s.addToken(topic, st, &tokenEntry{anchor: a, txid: txid, txidKey: hashKey(id), vout: vout, outpoint: op, height: h, index: i, inputs: inputsOf(tx)})
		return true, nil
	}
	// A funding output: held so that a later spend of it can be read, and
	// so that the tree class can answer its transaction.
	if !logrec.IsFundingShape(lock) {
		s.host.Log("ls_log indexed an output of no known kind", map[string]any{"topic": topic, "outpoint": op})
		return true, nil
	}
	k := topic + "|" + txid
	if s.trees[k] == nil {
		s.trees[k] = map[uint32]bool{}
	}
	s.trees[k][vout] = true
	return true, nil
}

// isPushDrop is logrec's lenient lock-before PushDrop reading: a 33-byte
// push of a compressed key prefix, then OP_CHECKSIG, then at least one push.
func isPushDrop(s []byte) bool {
	chunks, err := script.DecodeScript(s)
	if err != nil || len(chunks) < 3 {
		return false
	}
	k := chunks[0]
	if k.Op > script.OpPUSHDATA4 || len(k.Data) != 33 || (k.Data[0] != 0x02 && k.Data[0] != 0x03) {
		return false
	}
	return chunks[1].Op == script.OpCHECKSIG && chunks[2].Op <= script.OpPUSHDATA4
}

func (s *Service) hold(topic, op string, c logrec.Coin) {
	s.held[topic+"|"+op] = c
	s.ref(op, ref{topic: topic, kind: "coin"})
}

func (s *Service) ref(op string, r ref) {
	for _, x := range s.byOutpoint[op] {
		if x.topic == r.topic && x.kind == r.kind {
			return
		}
	}
	s.byOutpoint[op] = append(s.byOutpoint[op], r)
}

func (s *Service) streamOf(producer, name string) *stream {
	k := producer + "|" + name
	st := s.streams[k]
	if st == nil {
		st = &stream{producer: producer, stream: name, batches: map[uint64][]*batchEntry{}, tokens: map[uint64][]*tokenEntry{}}
		s.streams[k] = st
		if m, ok := s.marks[k]; ok {
			st.retiredInterval, st.retiredSeq, st.retiredC, st.highest = m.Interval, m.Seq, m.C, m.Seq
		}
	}
	return st
}

func (s *Service) addBatch(topic string, st *stream, e *batchEntry) {
	list := st.batches[e.seq]
	for _, x := range list {
		if x.cKey == e.cKey {
			return
		}
	}
	list = append(list, e)
	sort.Slice(list, func(i, j int) bool { return list[i].cKey < list[j].cKey })
	if len(list) > MaxCandidates {
		cut := list[len(list)-1]
		list = list[:len(list)-1]
		s.host.Metrics().Inc("blogs_candidates_dropped_total", nil, 1)
		s.host.Log("ls_log dropped a fork candidate over the cap", map[string]any{"stream": st.stream, "seq": e.seq, "txid": cut.txid})
		if cut == e {
			st.batches[e.seq] = list
			return
		}
		s.forget(cut)
	}
	if len(list) == 2 {
		s.forkSeqs++
	}
	st.batches[e.seq] = list
	if e.seq > st.highest {
		st.highest = e.seq
	}
	s.ref(opKey(e.txid, e.vout), ref{topic: topic, kind: "batch", stream: st})
	if s.carriersByFunding[e.funding] == nil {
		s.carriersByFunding[e.funding] = map[string]bool{}
	}
	s.carriersByFunding[e.funding][e.txid] = true
	s.checkRetracted(e.funding)
}

func (s *Service) forget(e *batchEntry) {
	if set := s.carriersByFunding[e.funding]; set != nil {
		delete(set, e.txid)
		if len(set) == 0 {
			delete(s.carriersByFunding, e.funding)
		}
	}
	delete(s.retracted, e.txid)
}

func (s *Service) addToken(topic string, st *stream, t *tokenEntry) {
	k := t.anchor.Interval
	list := st.tokens[k]
	for _, x := range list {
		if x.txid == t.txid {
			return
		}
	}
	list = append(list, t)
	st.tokens[k] = list
	st.chain = nil
	s.ref(t.outpoint, ref{topic: topic, kind: "token", stream: st})
	if len(list) > MaxCandidates {
		// Canonical first, then by txid in hash byte order: the last goes.
		var canonical *tokenEntry
		if c := s.chainOf(st).tokens; uint64(len(c)) >= k {
			canonical = c[k-1]
		}
		sortTokens(list, canonical)
		cut := list[len(list)-1]
		st.tokens[k] = list[:len(list)-1]
		st.chain = nil
		s.host.Metrics().Inc("blogs_tokens_dropped_total", nil, 1)
		s.host.Log("ls_log dropped an anchor token over the cap", map[string]any{"stream": st.stream, "interval": k, "txid": cut.txid})
	}
}

func sortTokens(list []*tokenEntry, canonical *tokenEntry) {
	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]
		if a == canonical {
			return b != canonical
		}
		if b == canonical {
			return false
		}
		return a.txidKey < b.txidKey
	})
}

// noteSpends records every input of an admitted transaction as a spend.
// Only a mined spend retracts: a carrier never mines, and an unmined spend
// of a funding output may never mine either (spec section 8.4).
func (s *Service) noteSpends(tx *transaction.Transaction) {
	txid := tx.TxID().String()
	if s.noted[txid] {
		return
	}
	s.noted[txid] = true
	mined := tx.MerklePath != nil
	for _, op := range inputsOf(tx) {
		m := s.spenders[op]
		if m == nil {
			m = map[string]bool{}
			s.spenders[op] = m
		}
		m[txid] = mined || m[txid]
		if mined {
			s.checkRetracted(op)
		}
	}
}

func (s *Service) checkRetracted(funding string) {
	carriers, spent := s.carriersByFunding[funding], s.spenders[funding]
	if carriers == nil || spent == nil {
		return
	}
	for _, c := range sortedKeys(carriers) {
		if s.retracted[c] {
			continue
		}
		for _, by := range sortedKeys(spent) {
			if !spent[by] || by == c {
				continue
			}
			s.retracted[c] = true
			s.host.Metrics().Inc("blogs_retractions_total", nil, 1)
			s.host.Log("ls_log retracted a batch", map[string]any{"carrier": c, "funding": funding, "by": by})
			break
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// dropOutpoint removes what was indexed at op, on topic or on every topic
// when topic is empty.
func (s *Service) dropOutpoint(op, topic string) {
	refs := s.byOutpoint[op]
	if refs == nil {
		return
	}
	var keep []ref
	for _, r := range refs {
		if topic != "" && r.topic != topic {
			keep = append(keep, r)
			continue
		}
		delete(s.held, r.topic+"|"+op)
		switch {
		case r.kind == "coin":
			s.dropTree(r.topic, op)
		case r.kind == "batch" && r.stream != nil:
			s.dropBatch(r.stream, op)
		case r.kind == "token" && r.stream != nil:
			s.dropToken(r.stream, op)
		}
	}
	if len(keep) == 0 {
		delete(s.byOutpoint, op)
	} else {
		s.byOutpoint[op] = keep
	}
}

func (s *Service) dropTree(topic, op string) {
	txid, vout := splitOp(op)
	k := topic + "|" + txid
	vouts := s.trees[k]
	if vouts == nil {
		return
	}
	delete(vouts, vout)
	if len(vouts) == 0 {
		delete(s.trees, k)
	}
}

func (s *Service) dropBatch(st *stream, op string) {
	for seq, list := range st.batches {
		for i, e := range list {
			if opKey(e.txid, e.vout) != op {
				continue
			}
			s.forget(e)
			list = append(list[:i:i], list[i+1:]...)
			if len(list) == 1 {
				s.forkSeqs--
			}
			if len(list) == 0 {
				delete(st.batches, seq)
				if seq == st.highest {
					st.highest = st.retiredSeq
					for k := range st.batches {
						if k > st.highest {
							st.highest = k
						}
					}
				}
			} else {
				st.batches[seq] = list
			}
			return
		}
	}
}

func (s *Service) dropToken(st *stream, op string) {
	for k, list := range st.tokens {
		for i, t := range list {
			if t.outpoint != op {
				continue
			}
			list = append(list[:i:i], list[i+1:]...)
			if len(list) == 0 {
				delete(st.tokens, k)
			} else {
				st.tokens[k] = list
			}
			st.chain = nil
			return
		}
	}
}

// ---- statuses

func (s *Service) batchStatus(st *stream, e *batchEntry) string {
	if s.retracted[e.txid] {
		return BatchRetracted
	}
	for _, o := range st.batches[e.seq] {
		if o.cKey != e.cKey {
			return BatchFork
		}
	}
	if e.seq == 1 {
		return BatchLinked
	}
	if e.seq-1 == st.retiredSeq {
		if e.prev == st.retiredC {
			return BatchLinked
		}
		return BatchFork
	}
	before := st.batches[e.seq-1]
	if len(before) == 0 {
		return BatchGap
	}
	for _, p := range before {
		if p.cKey == e.prev {
			return BatchLinked
		}
	}
	return BatchFork
}

// chainOf is the canonical chain (spec section 8.3): interval 1's token
// mined first, then for each k the token that spends the canonical k - 1
// token, if its first continues the predecessor's. A token that fails that
// join is kept and reported, and the chain stops before it.
func (s *Service) chainOf(st *stream) *chainCache {
	if st.chain != nil {
		return st.chain
	}
	c := &chainCache{joinFailed: map[string]bool{}}
	firsts := append([]*tokenEntry(nil), st.tokens[1]...)
	sort.SliceStable(firsts, func(i, j int) bool {
		a, b := firsts[i], firsts[j]
		if a.height != b.height {
			return a.height < b.height
		}
		if a.index != b.index {
			return a.index < b.index
		}
		return a.txidKey < b.txidKey
	})
	var pred *tokenEntry
	if len(firsts) > 0 {
		pred = firsts[0]
	}
	for pred != nil {
		c.tokens = append(c.tokens, pred)
		var next *tokenEntry
		for _, t := range st.tokens[pred.anchor.Interval+1] {
			for _, in := range t.inputs {
				if in == pred.outpoint {
					next = t
					break
				}
			}
			if next != nil {
				break
			}
		}
		if next == nil {
			break
		}
		if next.anchor.First != pred.anchor.First+pred.anchor.Count {
			c.joinFailed[next.txid] = true
			break
		}
		pred = next
	}
	st.chain = c
	return c
}

func (s *Service) tokenStatus(st *stream, t *tokenEntry) string {
	c := s.chainOf(st)
	k := t.anchor.Interval
	if uint64(len(c.tokens)) >= k && c.tokens[k-1] == t {
		return TokenCanonical
	}
	if c.joinFailed[t.txid] {
		return TokenJoin
	}
	if k > 1 {
		prev := hex.EncodeToString(t.anchor.Prev[:])
		held := false
		for _, p := range st.tokens[k-1] {
			if p.txidKey == prev {
				held = true
				break
			}
		}
		if !held {
			return TokenPending
		}
	}
	return TokenBranch
}

// intervalChoice is an interval's completeness (spec section 8.3): every
// covered seq held by a batch or a receipt, and one choice of candidates
// whose root is the token's. Each choice starts from a candidate at the
// last seq and follows prev back; a position with one candidate that prev
// does not select is tried anyway, so the root, not the link, decides.
// When complete it also answers the chosen candidates, in seq order.
func (s *Service) intervalChoice(st *stream, t *tokenEntry) (IntervalReport, []*batchEntry) {
	a := t.anchor
	last := a.First + a.Count - 1
	r := IntervalReport{Interval: a.Interval, First: a.First, Count: a.Count, Txid: t.txid, Missing: []uint64{}}
	if a.Interval <= st.retiredInterval {
		r.Status, r.Retired = IntervalComplete, true
		return r, nil
	}
	for seq := a.First; seq <= last && seq >= a.First; seq++ {
		if len(st.batches[seq]) == 0 {
			r.Missing = append(r.Missing, seq)
		}
		if seq == math.MaxUint64 {
			break
		}
	}
	if len(r.Missing) > 0 {
		r.Status = IntervalIncomplete
		return r, nil
	}
	for _, end := range st.batches[last] {
		chosen := []*batchEntry{end}
		for seq := last - 1; seq >= a.First && seq < last; seq-- {
			cands := st.batches[seq]
			var pick *batchEntry
			for _, c := range cands {
				if c.cKey == chosen[0].prev {
					pick = c
					break
				}
			}
			if pick == nil && len(cands) == 1 {
				pick = cands[0]
			}
			if pick == nil {
				break
			}
			chosen = append([]*batchEntry{pick}, chosen...)
			if seq == 0 {
				break
			}
		}
		if uint64(len(chosen)) != a.Count {
			continue
		}
		cs := make([][32]byte, len(chosen))
		for i, e := range chosen {
			cs[i] = e.c
		}
		if logrec.IntervalRoot(cs) == a.Root {
			r.Status = IntervalComplete
			return r, chosen
		}
	}
	r.Status = IntervalRootMismatch
	return r, nil
}

// Report is the statuses of every (producer, stream) held, or of one
// stream when name is not empty, ordered by producer then stream.
func (s *Service) Report(name string) []StreamReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []StreamReport{}
	for _, k := range sortedKeys(s.streams) {
		st := s.streams[k]
		if name != "" && st.stream != name {
			continue
		}
		r := StreamReport{Producer: st.producer, Stream: st.stream, Highest: st.highest, Holes: [][2]uint64{}, Forks: []uint64{}, Retracted: []uint64{}, Intervals: []IntervalReport{}, Tokens: []TokenReport{}}
		// Over the held seqs only: a producer may number a batch 2^53 - 1.
		// Seqs retention dropped are not holes.
		seqs := make([]uint64, 0, len(st.batches))
		for seq := range st.batches {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		last := st.retiredSeq
		for _, seq := range seqs {
			if seq > last+1 {
				r.Holes = append(r.Holes, [2]uint64{last + 1, seq - 1})
			}
			last = seq
			fork, all := false, true
			for _, e := range st.batches[seq] {
				switch s.batchStatus(st, e) {
				case BatchFork:
					fork, all = true, false
				case BatchRetracted:
				default:
					all = false
				}
			}
			if fork {
				r.Forks = append(r.Forks, seq)
			}
			if all {
				r.Retracted = append(r.Retracted, seq)
			}
		}
		for _, t := range s.chainOf(st).tokens {
			ir, _ := s.intervalChoice(st, t)
			r.Intervals = append(r.Intervals, ir)
		}
		for _, list := range st.tokens {
			for _, t := range list {
				if status := s.tokenStatus(st, t); status != TokenCanonical {
					r.Tokens = append(r.Tokens, TokenReport{Interval: t.anchor.Interval, Txid: t.txid, Status: status})
				}
			}
		}
		sort.Slice(r.Tokens, func(i, j int) bool {
			if r.Tokens[i].Interval != r.Tokens[j].Interval {
				return r.Tokens[i].Interval < r.Tokens[j].Interval
			}
			return r.Tokens[i].Txid < r.Tokens[j].Txid
		})
		out = append(out, r)
	}
	return out
}

// BatchStatusOf is the status of the batch whose carrier is txid (display
// hex) on a stream, if held.
func (s *Service) BatchStatusOf(producer, name, txid string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.streams[producer+"|"+name]
	if st == nil {
		return "", false
	}
	for _, list := range st.batches {
		for _, e := range list {
			if e.txid == txid {
				return s.batchStatus(st, e), true
			}
		}
	}
	return "", false
}

// ---- questions

// Formula is one output an answer names.
type Formula struct {
	Txid string
	Vout uint32
}

// Answer is the outputs that answer one question, in answer order.
func (s *Service) Answer(ctx context.Context, service string, query []byte) ([]Formula, error) {
	if service != logrec.LookupService {
		return nil, fmt.Errorf("ls_log: this service is %s, not %s", logrec.LookupService, service)
	}
	q, err := logrec.ParseQuery(query)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if q.Class == logrec.ClassTree {
		// One output answers the whole transaction: the engine hydrates it
		// as the tree's BEEF, with the proof it was admitted with.
		topic := logrec.TopicPrefix + q.Stream
		k := topic + "|" + q.Tree
		low, found := uint32(0), false
		for v := range s.trees[k] {
			if !found || v < low {
				low, found = v, true
			}
		}
		if v, ok := s.treeLow[k]; ok && (!found || v < low) {
			low, found = v, true
		}
		storage := s.storage
		s.mu.Unlock()
		if found {
			return []Formula{{Txid: q.Tree, Vout: low}}, nil
		}
		// A tree whose every carrier retention dropped is no longer in the
		// index; its outputs are still in storage, and output 0 is the
		// lowest there can be (a lower one dropped is in treeLow).
		if s.retention != nil && storage != nil {
			h, err := chainhash.NewHashFromHex(q.Tree)
			if err != nil {
				return []Formula{}, nil
			}
			o, err := storage.FindOutput(ctx, &transaction.Outpoint{Txid: *h, Index: 0}, topic)
			if err != nil {
				return nil, err
			}
			if o != nil && o.OutputScript != nil && logrec.IsFundingShape(*o.OutputScript) {
				return []Formula{{Txid: q.Tree, Vout: 0}}, nil
			}
		}
		return []Formula{}, nil
	}
	defer s.mu.Unlock()
	st := s.streams[hex.EncodeToString(q.Producer)+"|"+q.Stream]
	if st == nil {
		return []Formula{}, nil
	}
	out := []Formula{}
	if q.Class == logrec.ClassBatch {
		for _, e := range st.batches[q.Seq] {
			if !s.retracted[e.txid] {
				out = append(out, Formula{Txid: e.txid, Vout: e.vout})
			}
		}
		return out, nil
	}
	var canonical *tokenEntry
	if c := s.chainOf(st).tokens; uint64(len(c)) >= q.Interval {
		canonical = c[q.Interval-1]
	}
	list := append([]*tokenEntry(nil), st.tokens[q.Interval]...)
	sortTokens(list, canonical)
	for _, t := range list {
		out = append(out, Formula{Txid: t.txid, Vout: t.vout})
	}
	return out, nil
}

// Lookup implements module.LookupService: a formula answer, which the
// engine hydrates from its storage as the TypeScript engine does.
func (s *Service) Lookup(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
	if q == nil {
		return nil, errors.New("ls_log: no question")
	}
	fs, err := s.Answer(ctx, q.Service, q.Query)
	if err != nil {
		return nil, err
	}
	ans := &lookup.LookupAnswer{Type: lookup.AnswerTypeFormula, Formulas: make([]lookup.LookupFormula, 0, len(fs))}
	for _, f := range fs {
		h, err := chainhash.NewHashFromHex(f.Txid)
		if err != nil {
			return nil, err
		}
		ans.Formulas = append(ans.Formulas, lookup.LookupFormula{Outpoint: &transaction.Outpoint{Txid: *h, Index: f.Vout}})
	}
	return ans, nil
}

// Restored reports whether the host has handed storage over.
func (s *Service) Restored() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storage != nil
}

// GetDocumentation implements module.LookupService.
func (*Service) GetDocumentation() string {
	return strings.Join([]string{
		"# ls_log",
		"",
		"Three free question classes, for every blogs stream this host carries:",
		"",
		"- `{\"stream\": s, \"producer\": p, \"seq\": n}`: the admitted, unretracted batch",
		"  carriers of p on s at seq n (at most 8, by commitment);",
		"- `{\"stream\": s, \"producer\": p, \"interval\": k}`: the admitted anchor tokens of",
		"  p on s for interval k (at most 8, canonical first);",
		"- `{\"stream\": s, \"tree\": t}`: the mined funding tree with txid t whose funding",
		"  outputs the topic of s holds, spent or not (one output, the lowest held; its",
		"  BEEF is the tree with its proof).",
		"",
		"Any other member, a missing member or a value out of range is refused with",
		"an error. An empty answer means only that this host holds nothing that answers.",
	}, "\n")
}

// GetMetaData implements module.LookupService.
func (*Service) GetMetaData() *overlay.MetaData {
	return &overlay.MetaData{Name: logrec.LookupService, Description: "Answers blogs batches by seq, anchor tokens by interval and funding trees by txid."}
}
