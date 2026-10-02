// Package testchain is the local stand-ins this module's tests run against:
// the shared chain, and this application's host.
package testchain

import (
	"encoding/json"
	"net/http"

	bc "github.com/lightwebinc/bcommon/testchain"
)

// Chain is the shared local chain.
type Chain = bc.Chain

// CoinbaseValue and Maturity are the chain's.
const (
	CoinbaseValue = bc.CoinbaseValue
	Maturity      = bc.Maturity
)

// New is a chain whose tip is at height start.
func New(start uint32) *Chain { return bc.New(start) }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
