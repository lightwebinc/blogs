package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/lightwebinc/blogs/internal/ingest"
	"github.com/lightwebinc/blogs/internal/produce"
)

// retractAnswer is the admin API's answer to a retraction.
type retractAnswer struct {
	Stream string   `json:"stream"`
	Seqs   []uint64 `json:"seqs"`
	Tree   string   `json:"tree"`
	Sweep  string   `json:"sweep"`
	// State is "submitted": the sweep is persisted and on its way; the
	// sidecar settles and publishes it on its anchor ticks.
	State string `json:"state"`
}

// adminHandler serves the sidecar's admin API. POST
// /admin/retract?stream=S&seqs=A-B retracts the batches at those seqs while
// the sidecar keeps taking records: the plan (which funding outputs the
// carriers spent, asked of the hosts) and the sweep are built on the
// publishing goroutine between batches, and the sweep is then settled and
// published on the anchor ticks. One sweep is in flight at a time per
// stream, and one retraction names one funding tree.
func adminHandler(g *global, b *ingest.Batcher, engines []*produce.Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/retract", func(w http.ResponseWriter, r *http.Request) {
		stream := r.URL.Query().Get("stream")
		var e *produce.Engine
		for _, x := range engines {
			if x.St.Stream == stream {
				e = x
			}
		}
		if e == nil {
			http.Error(w, "no stream "+stream+" in this sidecar", http.StatusNotFound)
			return
		}
		lo, hi, err := seqRange(r.URL.Query().Get("seqs"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var ans retractAnswer
		status := http.StatusAccepted
		err = b.Do(r.Context(), func(ctx context.Context) error {
			if e.InFlight() {
				status = http.StatusConflict
				return errors.New("a sweep of this stream is still in flight; try again once it is mined")
			}
			plan, err := g.retractPlan(ctx, e, lo, hi)
			if err != nil {
				status = http.StatusUnprocessableEntity
				return err
			}
			if len(plan) != 1 {
				status = http.StatusBadRequest
				return errors.New("these seqs were funded by more than one tree; retract them one tree at a time")
			}
			for t, p := range plan {
				sw, err := e.Retract(ctx, t, p.vouts, p.seqs)
				if err != nil {
					status = http.StatusInternalServerError
					return err
				}
				seqs := append([]uint64(nil), p.seqs...)
				sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
				ans = retractAnswer{Stream: stream, Seqs: seqs, Tree: t, Sweep: sw.Txid, State: "submitted"}
				g.say("admin: retracting seqs %s of %s: sweep %s", joinU64(seqs), stream, sw.Txid)
			}
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(ans)
	})
	return mux
}
