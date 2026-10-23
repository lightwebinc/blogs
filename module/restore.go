package module

import (
	"context"
	"math"
	"sort"

	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Restore implements module.Restorer: it rebuilds the index from storage
// (spec section 8.5). The host hands the unspent outputs of this module's
// topics with their BEEF (a row handed without it is read again from
// storage with it); the index also needs spent ones (each anchor token but
// the newest is spent by the next, and a carrier's funding output by the
// carrier and by any sweep), so it walks from every row to the outputs its
// transaction spent (OutputsConsumed), the outputs that spent it
// (ConsumedBy), and its transaction's other outputs, asking storage for
// each. Every output connected to an unspent one is found, whatever order
// storage returns them in; statuses are functions of the set. Storage is
// kept for the topic managers' previous coins. A storage failure
// propagates: a restore that skipped a retraction would serve retracted
// batches.
func (s *Service) Restore(ctx context.Context, outputs []bmodule.Output, storage bmodule.RestoreStorage) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clear()
	s.storage = storage
	key := func(topic, op string) string { return topic + "|" + op }
	seen := map[string]bool{}
	var queue []bmodule.Output
	for _, o := range outputs {
		if !s.topics[o.Topic] {
			continue
		}
		seen[key(o.Topic, opKey(o.Outpoint.Txid.String(), o.Outpoint.Index))] = true
		queue = append(queue, o)
	}
	txs := map[string]*transaction.Transaction{}
	parsed := map[string]bool{}
	// Outputs storage no longer has (a carrier retention deleted), and the
	// funding outputs every spender of which is such an output.
	absent := map[string]bool{}
	type funding struct {
		topic, op string
		spenders  []string
	}
	var fundings []funding
	visit := func(topic string, op transaction.Outpoint) error {
		k := key(topic, opKey(op.Txid.String(), op.Index))
		if seen[k] {
			return nil
		}
		seen[k] = true
		o, err := storage.FindOutput(ctx, &op, topic)
		if err != nil {
			return err
		}
		if o != nil {
			queue = append(queue, *o)
		} else {
			absent[k] = true
		}
		return nil
	}
	indexed := 0
	for len(queue) > 0 {
		o := queue[0]
		queue = queue[1:]
		txid := o.Outpoint.Txid.String()
		if o.Beef == nil && !parsed[txid] {
			// A host that hands its unspent rows without their BEEF: read
			// the row again with it.
			full, err := storage.FindOutput(ctx, &o.Outpoint, o.Topic)
			if err != nil {
				return 0, err
			}
			if full != nil {
				o = *full
			}
		}
		first := !parsed[txid]
		if first {
			parsed[txid] = true
			if o.Beef != nil {
				if b, tx, _, err := transaction.ParseBeef(o.Beef); err == nil && tx != nil {
					if tx.TxID().IsEqual(&o.Outpoint.Txid) {
						txs[txid] = tx
					} else if t := b.FindTransactionByHash(&o.Outpoint.Txid); t != nil {
						txs[txid] = t
					}
				}
			}
		}
		tx := txs[txid]
		if tx == nil {
			s.host.Log("ls_log could not restore an output without its transaction", map[string]any{"topic": o.Topic, "txid": txid, "outputIndex": o.Outpoint.Index})
		} else {
			ok, err := s.index(o.Topic, tx, o.Outpoint.Index)
			switch {
			case err != nil:
				// Admission checked every stored output; one that no
				// longer decodes is logged and skipped rather than
				// stopping the start.
				s.host.Log("ls_log could not restore an output", map[string]any{"topic": o.Topic, "txid": txid, "outputIndex": o.Outpoint.Index, "err": err.Error()})
			case ok:
				s.noteSpends(tx)
				indexed++
				if s.retention != nil && len(o.ConsumedBy) > 0 && s.trees[o.Topic+"|"+txid][o.Outpoint.Index] {
					f := funding{topic: o.Topic, op: opKey(txid, o.Outpoint.Index)}
					for _, c := range o.ConsumedBy {
						if c != nil {
							f.spenders = append(f.spenders, key(o.Topic, opKey(c.Txid.String(), c.Index)))
						}
					}
					fundings = append(fundings, f)
				}
			}
		}
		for _, c := range o.OutputsConsumed {
			if c != nil {
				if err := visit(o.Topic, *c); err != nil {
					return 0, err
				}
			}
		}
		for _, c := range o.ConsumedBy {
			if c != nil {
				if err := visit(o.Topic, *c); err != nil {
					return 0, err
				}
			}
		}
		if first && tx != nil {
			for j := range tx.Outputs {
				if err := visit(o.Topic, transaction.Outpoint{Txid: o.Outpoint.Txid, Index: uint32(j)}); err != nil { //nolint:gosec // an output index
					return 0, err
				}
			}
		}
	}
	// A funding output spent only by carriers retention deleted: the tree
	// class still answers its tree, and the index need not hold it.
	for _, f := range fundings {
		all := true
		for _, k := range f.spenders {
			if !absent[k] {
				all = false
				break
			}
		}
		if all && len(s.carriersByFunding[f.op]) == 0 {
			s.retireFunding(f.topic, f.op)
		}
	}
	return indexed, nil
}

// Retain is one retention pass at the host's time: for each stream, its
// oldest intervals not yet dropped, in order, while each is canonical,
// complete and older than the policy, have every carrier dropped from the
// index; the state file is written; then the dropped carriers' outputs are
// deleted from storage. Nothing happens before a restore has handed storage
// over. Passes never overlap.
func (s *Service) Retain(ctx context.Context) RetentionPass {
	s.retainMu.Lock()
	defer s.retainMu.Unlock()
	var out RetentionPass
	r := s.retention
	if r == nil {
		return out
	}
	s.mu.Lock()
	if s.storage == nil {
		s.mu.Unlock()
		return out
	}
	now := r.Now()
	for _, k := range sortedKeys(s.streams) {
		st := s.streams[k]
		chain := s.chainOf(st).tokens
		for i := st.retiredInterval + 1; i <= uint64(len(chain)); i++ {
			t := chain[i-1]
			if float64(now)-float64(t.anchor.Timestamp) < r.KeepMs {
				break
			}
			report, chosen := s.intervalChoice(st, t)
			if report.Status != IntervalComplete || chosen == nil {
				break
			}
			topic := logrec.TopicPrefix + st.stream
			last := t.anchor.First + t.anchor.Count - 1
			for seq := t.anchor.First; seq <= last && seq >= t.anchor.First; seq++ {
				list := st.batches[seq]
				if len(list) > 1 {
					s.forkSeqs--
				}
				for _, e := range list {
					s.retireBatch(topic, e)
					out.Carriers++
				}
				delete(st.batches, seq)
				if seq == math.MaxUint64 {
					break
				}
			}
			st.retiredInterval = i
			st.retiredSeq = last
			st.retiredC = chosen[len(chosen)-1].cKey
			s.marks[st.producer+"|"+st.stream] = Mark{Producer: st.producer, Stream: st.stream, Interval: i, Seq: st.retiredSeq, C: st.retiredC}
			s.stateDirty = true
			out.Intervals++
		}
	}
	if out.Carriers > 0 {
		s.host.Metrics().Inc("blogs_retired_total", nil, float64(out.Carriers))
		s.host.Log("ls_log retention dropped proven carriers", map[string]any{"intervals": out.Intervals, "carriers": out.Carriers})
	}
	if s.stateDirty {
		// Written before anything is deleted: a crash between the two
		// leaves carriers in storage that restore skips and the next pass
		// deletes, never a deleted carrier counted as a hole. Until it is
		// written, nothing is deleted.
		if err := r.Store.Save(s.stateLocked()); err != nil {
			s.host.Log("ls_log could not write the retention state; nothing is deleted from storage until it is", map[string]any{"err": err.Error()})
			s.mu.Unlock()
			return out
		}
		s.stateDirty = false
	}
	queue := s.drops
	s.drops = nil
	storage := s.storage
	s.mu.Unlock()
	out.Deleted = s.deleteDrops(ctx, storage, queue)
	return out
}

func (s *Service) stateLocked() RetentionState {
	var st RetentionState
	for _, k := range sortedKeys(s.marks) {
		st.Marks = append(st.Marks, s.marks[k])
	}
	for _, k := range sortedKeys(s.treeLow) {
		st.Trees = append(st.Trees, TreeLow{Key: k, Vout: s.treeLow[k]})
	}
	return st
}

// retireBatch takes a carrier out of the index, with its funding output
// when no other carrier spends it, and queues its output for deletion.
func (s *Service) retireBatch(topic string, e *batchEntry) {
	s.forget(e)
	op := opKey(e.txid, e.vout)
	delete(s.held, topic+"|"+op)
	s.unref(op, topic)
	delete(s.noted, e.txid)
	if o, ok := parseOp(op); ok {
		s.drops = append(s.drops, drop{op: o, topic: topic})
	}
	if len(s.carriersByFunding[e.funding]) == 0 {
		s.retireFunding(topic, e.funding)
	}
}

// retireFunding takes a funding output out of the index. Storage keeps it,
// spent, with its transaction (spec section 8.1); the tree class answers
// its tree from what is left of it in the index, from treeLow, or from
// storage.
func (s *Service) retireFunding(topic, op string) {
	delete(s.held, topic+"|"+op)
	s.unref(op, topic)
	delete(s.spenders, op)
	txid, vout := splitOp(op)
	k := topic + "|" + txid
	vouts := s.trees[k]
	if vouts == nil || !vouts[vout] {
		return
	}
	delete(vouts, vout)
	low := vout
	if v, ok := s.treeLow[k]; ok && v < low {
		low = v
	}
	if len(vouts) > 0 || low != 0 {
		s.treeLow[k] = low
	} else {
		delete(s.treeLow, k)
	}
	if len(vouts) == 0 {
		delete(s.trees, k)
	}
	s.stateDirty = true
}

func (s *Service) unref(op, topic string) {
	refs := s.byOutpoint[op]
	if refs == nil {
		return
	}
	var keep []ref
	for _, r := range refs {
		if r.topic != topic {
			keep = append(keep, r)
		}
	}
	if len(keep) == 0 {
		delete(s.byOutpoint, op)
	} else {
		s.byOutpoint[op] = keep
	}
}

// deleteDrops deletes the queued carrier outputs from storage; one that
// fails stays queued.
func (s *Service) deleteDrops(ctx context.Context, storage bmodule.RestoreStorage, queue []drop) int {
	if len(queue) == 0 || storage == nil {
		return 0
	}
	del, ok := storage.(Deleter)
	if !ok {
		s.mu.Lock()
		if !s.warned["delete"] {
			s.warned["delete"] = true
			s.host.Log("ls_log cannot delete dropped carriers: the storage has no DeleteOutput; they stay in storage and restore skips them", nil)
		}
		s.mu.Unlock()
		return 0
	}
	sort.SliceStable(queue, func(i, j int) bool { return queue[i].op.String() < queue[j].op.String() })
	n := 0
	var failed []drop
	for _, d := range queue {
		op := d.op
		if err := del.DeleteOutput(ctx, &op, d.topic); err != nil {
			failed = append(failed, d)
			s.host.Log("ls_log could not delete a dropped carrier; the next pass tries again", map[string]any{"txid": op.Txid.String(), "err": err.Error()})
			continue
		}
		n++
	}
	if len(failed) > 0 {
		s.mu.Lock()
		s.drops = append(s.drops, failed...)
		s.mu.Unlock()
	}
	return n
}
