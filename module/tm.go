package module

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Reasons are every refusal label, for presetting the refusal counter.
var Reasons = []string{
	"too-large", "cbor", "key-type", "magic", "missing", "type", "range", "producer", "stream", "prev", "profile-shape",
	"locator", "content", "carrier-shape", "anchor-shape", "producer-policy", "mineable", "lock", "signature", "funding",
	"unlock", "anchor-input", "unmined",
}

// DeferredReasons are refusals whose verdict can change with what the host
// holds or its headers.
var DeferredReasons = []string{"anchor-input", "unmined"}

// AdmitKinds are what an accepted transaction was: a batch carrier, an
// anchor token, a mined funding tree or sweep, a spend of held outputs, or
// none of these (it claims nothing, spends nothing held and admits nothing).
var AdmitKinds = []string{"batch", "anchor", "funding", "spend", "none"}

// CoinSource is the output the topic holds at an outpoint, or false. The
// lookup service provides it: from the engine's storage once the host has
// handed it over, otherwise from the outputs it has indexed.
type CoinSource func(ctx context.Context, topic string, op transaction.Outpoint) (logrec.Coin, bool, error)

// Unrecorded is raised for a refusal the engine must not record: the
// engine counts it as a failed topic for this submission and stores
// nothing.
type Unrecorded struct {
	Reason string
	Detail string
}

func (u *Unrecorded) Error() string {
	return "blogs: " + u.Reason + ", not recorded: " + u.Detail
}

// engineVerified is the tracker the managers use on a host. The engine has
// verified a submitted transaction's proof against the host's headers
// before calling a topic manager; what remains for rule 8 and the funding
// tree rule is that the proof is present and names the transaction, which
// MerklePath.Verify checks before it asks a tracker. This tracker answers
// yes to what the engine already checked.
type engineVerified struct{}

func (engineVerified) IsValidRootForHeight(context.Context, *chainhash.Hash, uint32) (bool, error) {
	return true, nil
}

func (engineVerified) CurrentHeight(context.Context) (uint32, error) { return 0, nil }

// EngineVerified is engineVerified as a chain tracker.
var EngineVerified chaintracker.ChainTracker = engineVerified{}

// TopicManager is tm_log_<name>_<suffix>: the topic manager of one stream
// (spec section 8.1). The rules are logrec.Admit's; this is the engine seam:
// it reads the previous coins, runs the rules, and turns the verdict into
// admittance instructions.
//
// How a refusal reaches the engine matters, because the engine records
// every transaction a topic manager returns instructions for as applied,
// empty instructions included, and treats a later submission of the same
// transaction as a duplicate that no topic manager sees again. It also
// marks spent, or deletes, every held output the transaction spends. A
// refusal is therefore returned (recorded) only when its verdict can never
// change and the transaction spends nothing the topic holds. Otherwise it
// is raised, and the engine records nothing.
type TopicManager struct {
	Topic   string
	Stream  string
	policy  logrec.Policy
	coins   CoinSource
	host    bmodule.Host
	headers chaintracker.ChainTracker
}

var (
	_ bmodule.TopicManager    = (*TopicManager)(nil)
	_ bmodule.InputIdentifier = (*TopicManager)(nil)
)

// NewTopicManager makes the manager of one topic. headers nil is the
// engine-verified tracker a host uses.
func NewTopicManager(topic string, policy logrec.Policy, coins CoinSource, host bmodule.Host, headers chaintracker.ChainTracker) (*TopicManager, error) {
	if !strings.HasPrefix(topic, logrec.TopicPrefix) {
		return nil, fmt.Errorf("blogs: %s is not a tm_log_ topic", topic)
	}
	stream := topic[len(logrec.TopicPrefix):]
	if logrec.CheckStream(stream) != nil {
		return nil, fmt.Errorf("blogs: %s is not a stream identifier <name>_<suffix>", stream)
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if headers == nil {
		headers = EngineVerified
	}
	return &TopicManager{Topic: topic, Stream: stream, policy: policy, coins: coins, host: host, headers: headers}, nil
}

// ErrNoTransaction is a submission whose BEEF names no transaction.
var ErrNoTransaction = errors.New("blogs: the submission's BEEF holds no transaction")

// Admit runs the rules on one submission's BEEF with the previous coins (by
// input index), as the TypeScript manager's identifyAdmissibleOutputs does.
func (t *TopicManager) Admit(ctx context.Context, beef []byte, previousCoins []uint32) (overlay.AdmittanceInstructions, error) {
	_, tx, _, err := transaction.ParseBeef(beef)
	if err != nil {
		return overlay.AdmittanceInstructions{}, fmt.Errorf("blogs: the submission's BEEF: %w", err)
	}
	if tx == nil {
		return overlay.AdmittanceInstructions{}, ErrNoTransaction
	}
	return t.admitTx(ctx, tx, previousCoins)
}

func (t *TopicManager) admitTx(ctx context.Context, tx *transaction.Transaction, previousCoins []uint32) (overlay.AdmittanceInstructions, error) {
	txid := tx.TxID().String()
	coins := logrec.Coins{}
	for _, i := range previousCoins {
		if int(i) >= len(tx.Inputs) {
			continue
		}
		in := tx.Inputs[i]
		if in.SourceTXID == nil {
			continue
		}
		op := transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}
		c, ok, err := t.coins(ctx, t.Topic, op)
		if err != nil {
			return overlay.AdmittanceInstructions{}, err
		}
		if ok {
			coins[op] = c
		}
	}
	a, err := logrec.Admit(ctx, tx, coins, logrec.Host{Stream: t.Stream, Policy: t.policy, Headers: t.headers})
	if err != nil {
		reason := logrec.Reason(err)
		if reason == "other" {
			return overlay.AdmittanceInstructions{}, err
		}
		recorded := len(previousCoins) == 0 && !slices.Contains(DeferredReasons, reason)
		yes := "no"
		if recorded {
			yes = "yes"
		}
		t.host.Metrics().Inc("blogs_refused_total", map[string]string{"reason": reason, "recorded": yes}, 1)
		t.host.Log("tm_log refused", map[string]any{"topic": t.Topic, "txid": txid, "reason": reason, "detail": err.Error(), "recorded": recorded})
		if !recorded {
			detail := txid + " may be decided later"
			if len(previousCoins) > 0 {
				detail = txid + " spends held outputs"
			}
			return overlay.AdmittanceInstructions{}, &Unrecorded{Reason: reason, Detail: detail}
		}
		return overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{}, CoinsToRetain: []uint32{}}, nil
	}
	var kind string
	admit := []uint32{}
	switch {
	case a.Kind == logrec.KindBatch:
		kind, admit = "batch", []uint32{0}
	case a.Kind == logrec.KindAnchor:
		kind, admit = "anchor", []uint32{uint32(a.Token.OutputIndex)} //nolint:gosec // an output index
	case len(a.Funding) > 0:
		kind = "funding"
		for _, i := range a.Funding {
			admit = append(admit, uint32(i)) //nolint:gosec // an output index
		}
	case len(previousCoins) > 0:
		kind = "spend"
	default:
		kind = "none"
	}
	t.host.Metrics().Inc("blogs_admitted_total", map[string]string{"kind": kind}, 1)
	// Every held output an accepted transaction spends is retained, so that
	// no admitted output is deleted when something spends it (spec section
	// 8.1, retention and spends).
	return overlay.AdmittanceInstructions{OutputsToAdmit: admit, CoinsToRetain: append([]uint32{}, previousCoins...)}, nil
}

// IdentifyAdmissibleOutputs implements bcommon's module.TopicManager. The
// rules read the submission as it arrived (bcommon module.WithSubmission),
// as the TypeScript manager reads the submitted BEEF; a host that does not
// hand the bytes over is answered from the parsed BEEF's transaction.
func (t *TopicManager) IdentifyAdmissibleOutputs(ctx context.Context, beef *transaction.Beef, txid *chainhash.Hash, previousCoins []uint32) (overlay.AdmittanceInstructions, error) {
	if raw, ok := bmodule.Submission(ctx); ok {
		return t.Admit(ctx, raw, previousCoins)
	}
	if beef == nil || txid == nil {
		return overlay.AdmittanceInstructions{}, ErrNoTransaction
	}
	tx := beef.FindTransactionByHash(txid)
	if tx == nil {
		return overlay.AdmittanceInstructions{}, ErrNoTransaction
	}
	return t.admitTx(ctx, tx, previousCoins)
}

// IdentifyNeededInputs implements module.InputIdentifier: nothing is
// needed, since a carrier carries its ancestry and an anchor's spent
// outputs are held.
func (*TopicManager) IdentifyNeededInputs(context.Context, *transaction.Beef, *chainhash.Hash) ([]*transaction.Outpoint, error) {
	return nil, nil
}

// GetDocumentation implements module.TopicManager.
func (t *TopicManager) GetDocumentation() string {
	return strings.Join([]string{
		"# " + t.Topic,
		"",
		"The blogs stream " + t.Stream + ": batch carriers, mined anchor tokens, and the",
		"funding outputs of mined funding trees and sweeps, each admitted on its own",
		"validity (spec section 8.1). A transaction that spends held outputs and claims",
		"nothing is accepted for its inputs alone. Gaps and forks between batches are",
		"reported by ls_log, never refused.",
	}, "\n")
}

// GetMetaData implements module.TopicManager.
func (t *TopicManager) GetMetaData() *overlay.MetaData {
	return &overlay.MetaData{Name: t.Topic, Description: "Admits the blogs log stream " + t.Stream + "."}
}
