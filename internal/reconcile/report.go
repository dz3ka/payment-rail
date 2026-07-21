package reconcile

import (
	"fmt"
	"io"
	"math/big"
	"sort"
	"time"
)

// AddressBalance is one treasury address's on-chain balance for a single asset,
// as read by WP1's balanceOf path. Actual is in the asset's base (minor) units
// and uses big.Int because an on-chain balance can, in principle, exceed int64.
type AddressBalance struct {
	Address string
	Actual  *big.Int
}

// AssetReconciliation is the per-asset row of the report: what the ledger expects
// on-chain, what is actually on-chain, the reconciliation gap, and the
// proof-of-reserves verdict against user liabilities.
type AssetReconciliation struct {
	Asset                  string
	Addresses              []AddressBalance
	ExpectedFinalizedMinor int64    // finalized settlement value — ledger truth
	ConfirmedPendingMinor  int64    // settled-not-final: a reconciling BRIDGE term, not a discrepancy
	ActualOnChain          *big.Int // Σ addresses' balanceOf
	Discrepancy            *big.Int // ActualOnChain − (ExpectedFinalized + ConfirmedPending); 0 == reconciled
	LiabilitiesMinor       int64    // −Σ(non-house balances) for this asset
	Verdict                string   // "OK" if ActualOnChain ≥ LiabilitiesMinor else "UNDERCOLLATERALIZED"
}

// Report is the whole proof-of-reserves snapshot across all assets.
type Report struct {
	GeneratedAt time.Time
	Assets      []AssetReconciliation
	Clean       bool
}

const (
	verdictOK              = "OK"
	verdictUndercollateral = "UNDERCOLLATERALIZED"
)

// BuildReport folds the three per-asset inputs — ledger sums, user liabilities,
// and on-chain address balances — into one sorted Report.
//
// Discrepancy is the reconciliation identity:
//
//	Discrepancy = ActualOnChain − ExpectedFinalizedMinor − ConfirmedPendingMinor
//
// Confirmed-but-not-final funds are ALREADY on-chain yet not yet final in the
// ledger, so they are a bridge term on the expected side, never a discrepancy: a
// treasury holding only tracked settlement funds nets to Discrepancy == 0.
//
// Verdict is an independent proof-of-reserves check: reserves (ActualOnChain)
// must cover user liabilities regardless of the reconciliation gap. A system can
// reconcile to zero yet still be UNDERCOLLATERALIZED.
//
// Clean is true only if every asset both reconciles (Discrepancy == 0) and is
// collateralized (Verdict == OK).
func BuildReport(now time.Time, sums map[string]AssetSums, liabilities map[string]int64, actuals map[string][]AddressBalance) Report {
	assets := unionKeys(sums, liabilities, actuals)

	r := Report{GeneratedAt: now, Clean: true}
	for _, asset := range assets {
		s := sums[asset]
		liab := liabilities[asset]
		addrs := actuals[asset]

		actual := big.NewInt(0)
		for _, ab := range addrs {
			if ab.Actual != nil {
				actual.Add(actual, ab.Actual)
			}
		}

		// expected = finalized + confirmed-pending (bridge); Discrepancy is the
		// signed gap between on-chain reality and that expectation.
		expected := big.NewInt(s.FinalizedMinor)
		expected.Add(expected, big.NewInt(s.ConfirmedMinor))
		discrepancy := new(big.Int).Sub(actual, expected)

		// Proof of reserves: actual on-chain must cover user liabilities.
		verdict := verdictOK
		if actual.Cmp(big.NewInt(liab)) < 0 {
			verdict = verdictUndercollateral
		}

		if discrepancy.Sign() != 0 || verdict != verdictOK {
			r.Clean = false
		}

		r.Assets = append(r.Assets, AssetReconciliation{
			Asset:                  asset,
			Addresses:              addrs,
			ExpectedFinalizedMinor: s.FinalizedMinor,
			ConfirmedPendingMinor:  s.ConfirmedMinor,
			ActualOnChain:          actual,
			Discrepancy:            discrepancy,
			LiabilitiesMinor:       liab,
			Verdict:                verdict,
		})
	}
	return r
}

// unionKeys returns the sorted union of asset keys across the three input maps,
// so an asset present in only one dimension (e.g. liabilities but no settlements)
// still gets a report row.
func unionKeys(sums map[string]AssetSums, liabilities map[string]int64, actuals map[string][]AddressBalance) []string {
	set := make(map[string]struct{})
	for k := range sums {
		set[k] = struct{}{}
	}
	for k := range liabilities {
		set[k] = struct{}{}
	}
	for k := range actuals {
		set[k] = struct{}{}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// WriteText renders the report in a human-readable form. This is the report
// artifact, so amounts are shown here (minor units for ledger figures; the raw
// on-chain integer for balances). No JSON mode yet (deferred).
func (r Report) WriteText(w io.Writer) {
	fmt.Fprintf(w, "Reconciliation report — generated %s\n", r.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(w, "========================================================\n")
	for _, a := range r.Assets {
		fmt.Fprintf(w, "\nAsset: %s\n", a.Asset)
		fmt.Fprintf(w, "  expected finalized (minor):   %d\n", a.ExpectedFinalizedMinor)
		fmt.Fprintf(w, "  confirmed-pending (bridge):   %d\n", a.ConfirmedPendingMinor)
		fmt.Fprintf(w, "  actual on-chain:              %s\n", a.ActualOnChain.String())
		for _, ab := range a.Addresses {
			bal := "0"
			if ab.Actual != nil {
				bal = ab.Actual.String()
			}
			fmt.Fprintf(w, "    - %s: %s\n", ab.Address, bal)
		}
		fmt.Fprintf(w, "  discrepancy:                  %s\n", a.Discrepancy.String())
		fmt.Fprintf(w, "  liabilities (minor):          %d\n", a.LiabilitiesMinor)
		fmt.Fprintf(w, "  proof-of-reserves verdict:    %s\n", a.Verdict)
	}
	fmt.Fprintf(w, "\n========================================================\n")
	if r.Clean {
		fmt.Fprintf(w, "CLEAN — all assets reconciled and collateralized\n")
	} else {
		fmt.Fprintf(w, "DISCREPANCIES-FOUND — review flagged assets above\n")
	}
}
