package testchain

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/verify"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Host is a stand-in overlay host for blogs streams: /submit runs the
// engine's SPV and the blogs topic manager (logrec.Admit) over the topic's
// previous coins, and /lookup answers the two ls_log classes. Refusals the
// specification says leave no record (anchor-input, unmined) are not
// recorded, so the same transaction offered again is decided afresh.
// Retraction, restore and the link statuses are the real host module's and
// are not modelled.
type Host struct {
	mu      sync.Mutex
	chain   *Chain
	seen    map[string]bool
	held    map[transaction.Outpoint]logrec.Coin
	beef    map[string][]byte
	batches map[string][]answer
	tokens  map[string][]answer
	// Down answers every request with 503.
	Down bool
	// Drop, when set, is asked about each submission; true discards it
	// unseen, as a plane that lost the object would.
	Drop func(tx *transaction.Transaction) bool
	// Submits counts submissions; Refused counts refusals by reason.
	Submits int
	Refused map[string]int
}

type answer struct {
	txid string
	out  uint32
}

// NewHost is a host whose engine checks proofs against chain.
func NewHost(chain *Chain) *Host {
	return &Host{chain: chain, seen: map[string]bool{}, held: map[transaction.Outpoint]logrec.Coin{},
		beef: map[string][]byte{}, batches: map[string][]answer{}, tokens: map[string][]answer{}, Refused: map[string]int{}}
}

func key(producer []byte, stream string, n uint64) string {
	return fmt.Sprintf("%x/%s/%d", producer, stream, n)
}

func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	down := h.Down
	h.mu.Unlock()
	if down {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/submit":
		h.submit(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/lookup":
		h.lookup(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Host) submit(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	topic := r.Header.Get("x-topics")
	stream := strings.TrimPrefix(topic, logrec.TopicPrefix)
	steak := func(admit []uint32) {
		if admit == nil {
			admit = []uint32{}
		}
		writeJSON(w, map[string]any{topic: map[string]any{"outputsToAdmit": admit, "coinsToRetain": []uint32{}}})
	}
	_, tx, _, err := transaction.ParseBeef(body)
	if err != nil || tx == nil || !strings.HasPrefix(topic, logrec.TopicPrefix) {
		http.Error(w, "bad submission", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Submits++
	if h.Drop != nil && h.Drop(tx) {
		steak(nil)
		return
	}
	id := tx.TxID().String()
	if h.seen[id] {
		steak(nil)
		return
	}
	ctx := context.Background()
	if v, err := verify.Check(ctx, tx, h.chain); v != verify.Passed {
		http.Error(w, "spv: "+err.Error(), http.StatusBadRequest)
		return
	}
	coins := logrec.Coins{}
	for _, in := range tx.Inputs {
		op := transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}
		if c, ok := h.held[op]; ok {
			coins[op] = c
		}
	}
	adm, err := logrec.Admit(ctx, tx, coins, logrec.Host{Stream: stream, Policy: logrec.Policy{Open: true}, Headers: h.chain})
	if err != nil {
		reason := logrec.Reason(err)
		h.Refused[reason]++
		if reason != "anchor-input" && reason != "unmined" && len(coins) == 0 {
			h.seen[id] = true
		}
		steak(nil)
		return
	}
	h.seen[id] = true
	h.beef[id] = body
	hold := func(i uint32) {
		o := tx.Outputs[i]
		h.held[transaction.Outpoint{Txid: *tx.TxID(), Index: i}] = logrec.Coin{LockingScript: *o.LockingScript, Satoshis: o.Satoshis}
	}
	var admitted []uint32
	switch adm.Kind {
	case logrec.KindBatch:
		b := adm.Carrier.Batch
		k := key(b.Producer, b.Stream, b.Seq)
		h.batches[k] = append(h.batches[k], answer{id, 0})
		hold(0)
		admitted = []uint32{0}
	case logrec.KindAnchor:
		a := adm.Token.Anchor
		k := key(a.Producer, a.Stream, a.Interval)
		at := uint32(adm.Token.OutputIndex) //nolint:gosec // an output index
		h.tokens[k] = append(h.tokens[k], answer{id, at})
		hold(at)
		admitted = []uint32{at}
	default:
		for _, i := range adm.Funding {
			hold(uint32(i)) //nolint:gosec // an output index
			admitted = append(admitted, uint32(i))
		}
	}
	steak(admitted)
}

func (h *Host) lookup(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Service string `json:"service"`
		Query   struct {
			Stream   string `json:"stream"`
			Producer string `json:"producer"`
			Seq      uint64 `json:"seq"`
			Interval uint64 `json:"interval"`
		} `json:"query"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&q); err != nil || q.Service != logrec.LookupService {
		http.Error(w, `{"error":"bad question"}`, http.StatusBadRequest)
		return
	}
	p, err := hex.DecodeString(q.Query.Producer)
	if err != nil || (q.Query.Seq == 0) == (q.Query.Interval == 0) {
		http.Error(w, `{"error":"bad question"}`, http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var as []answer
	if q.Query.Seq > 0 {
		as = h.batches[key(p, q.Query.Stream, q.Query.Seq)]
	} else {
		as = h.tokens[key(p, q.Query.Stream, q.Query.Interval)]
	}
	sort.Slice(as, func(i, j int) bool { return as[i].txid < as[j].txid })
	type item struct {
		Beef        []int  `json:"beef"`
		OutputIndex uint32 `json:"outputIndex"`
	}
	outs := []item{}
	for _, a := range as {
		b := h.beef[a.txid]
		nums := make([]int, len(b))
		for i, c := range b {
			nums[i] = int(c)
		}
		outs = append(outs, item{nums, a.out})
	}
	writeJSON(w, map[string]any{"type": "output-list", "outputs": outs})
}

// Forget drops what the host holds for one seq, as a host that missed it.
func (h *Host) Forget(producer []byte, stream string, seq uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, a := range h.batches[key(producer, stream, seq)] {
		delete(h.seen, a.txid)
	}
	delete(h.batches, key(producer, stream, seq))
}

// Holds reports whether the host answers seq.
func (h *Host) Holds(producer []byte, stream string, seq uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.batches[key(producer, stream, seq)]) > 0
}

// SetDown sets Down under the host's lock.
func (h *Host) SetDown(down bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Down = down
}
