package reconcile

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"
)

func addr(a string, v int64) AddressBalance {
	return AddressBalance{Address: a, Actual: big.NewInt(v)}
}

// findAsset returns the reconciliation for an asset, failing the test if absent.
func findAsset(t *testing.T, r Report, asset string) AssetReconciliation {
	t.Helper()
	for _, a := range r.Assets {
		if a.Asset == asset {
			return a
		}
	}
	t.Fatalf("asset %q not in report", asset)
	return AssetReconciliation{}
}

func TestBuildReport_Clean(t *testing.T) {
	// actual == finalized+confirmed, liabilities <= actual.
	sums := map[string]AssetSums{"USDC": {FinalizedMinor: 1000, ConfirmedMinor: 37}}
	liab := map[string]int64{"USDC": 500}
	actuals := map[string][]AddressBalance{"USDC": {addr("0xA", 1037)}}

	r := BuildReport(time.Unix(0, 0), sums, liab, actuals)
	a := findAsset(t, r, "USDC")
	if a.Discrepancy.Sign() != 0 {
		t.Errorf("Discrepancy = %s, want 0", a.Discrepancy)
	}
	if a.Verdict != "OK" {
		t.Errorf("Verdict = %q, want OK", a.Verdict)
	}
	if !r.Clean {
		t.Errorf("Clean = false, want true")
	}
}

func TestBuildReport_Shortfall(t *testing.T) {
	sums := map[string]AssetSums{"USDC": {FinalizedMinor: 1000}}
	liab := map[string]int64{"USDC": 0}
	actuals := map[string][]AddressBalance{"USDC": {addr("0xA", 800)}}

	r := BuildReport(time.Unix(0, 0), sums, liab, actuals)
	a := findAsset(t, r, "USDC")
	if a.Discrepancy.Sign() >= 0 {
		t.Errorf("Discrepancy = %s, want negative", a.Discrepancy)
	}
	if r.Clean {
		t.Errorf("Clean = true, want false")
	}
}

func TestBuildReport_Surplus(t *testing.T) {
	sums := map[string]AssetSums{"USDC": {FinalizedMinor: 1000}}
	liab := map[string]int64{"USDC": 0}
	actuals := map[string][]AddressBalance{"USDC": {addr("0xA", 1200)}}

	r := BuildReport(time.Unix(0, 0), sums, liab, actuals)
	a := findAsset(t, r, "USDC")
	if a.Discrepancy.Sign() <= 0 {
		t.Errorf("Discrepancy = %s, want positive", a.Discrepancy)
	}
	if r.Clean {
		t.Errorf("Clean = true, want false")
	}
}

func TestBuildReport_ConfirmedNotFinalBridge(t *testing.T) {
	// Only 'settled' funds, no finalized. actual == confirmed => the in-flight
	// value is a bridge term and Discrepancy must be 0 (NOT flagged).
	sums := map[string]AssetSums{"USDC": {ConfirmedMinor: 250}}
	liab := map[string]int64{"USDC": 250}
	actuals := map[string][]AddressBalance{"USDC": {addr("0xA", 250)}}

	r := BuildReport(time.Unix(0, 0), sums, liab, actuals)
	a := findAsset(t, r, "USDC")
	if a.Discrepancy.Sign() != 0 {
		t.Errorf("Discrepancy = %s, want 0 (confirmed-not-final is a bridge)", a.Discrepancy)
	}
	if a.Verdict != "OK" || !r.Clean {
		t.Errorf("Verdict=%q Clean=%v, want OK/true", a.Verdict, r.Clean)
	}
}

func TestBuildReport_Undercollateralized(t *testing.T) {
	// actual == finalized+confirmed (Discrepancy 0) but actual < liabilities:
	// proof-of-reserves fails even though reconciliation nets to zero.
	sums := map[string]AssetSums{"USDC": {FinalizedMinor: 100}}
	liab := map[string]int64{"USDC": 5000}
	actuals := map[string][]AddressBalance{"USDC": {addr("0xA", 100)}}

	r := BuildReport(time.Unix(0, 0), sums, liab, actuals)
	a := findAsset(t, r, "USDC")
	if a.Discrepancy.Sign() != 0 {
		t.Errorf("Discrepancy = %s, want 0", a.Discrepancy)
	}
	if a.Verdict != "UNDERCOLLATERALIZED" {
		t.Errorf("Verdict = %q, want UNDERCOLLATERALIZED", a.Verdict)
	}
	if r.Clean {
		t.Errorf("Clean = true, want false")
	}
}

func TestBuildReport_UnionOfKeysSorted(t *testing.T) {
	// Keys present in different maps must all appear, sorted.
	sums := map[string]AssetSums{"USDT": {FinalizedMinor: 1}}
	liab := map[string]int64{"USDC": 0}
	actuals := map[string][]AddressBalance{"DAI": {addr("0xA", 0)}}

	r := BuildReport(time.Unix(0, 0), sums, liab, actuals)
	got := []string{}
	for _, a := range r.Assets {
		got = append(got, a.Asset)
	}
	want := []string{"DAI", "USDC", "USDT"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("assets = %v, want %v", got, want)
	}
}

func TestReport_WriteText(t *testing.T) {
	sums := map[string]AssetSums{"USDC": {FinalizedMinor: 1000, ConfirmedMinor: 37}}
	liab := map[string]int64{"USDC": 500}
	actuals := map[string][]AddressBalance{"USDC": {addr("0xABC", 1037)}}
	r := BuildReport(time.Unix(1700000000, 0).UTC(), sums, liab, actuals)

	var buf bytes.Buffer
	r.WriteText(&buf)
	out := buf.String()
	for _, want := range []string{"USDC", "1000", "37", "1037", "0xABC", "500", "OK", "CLEAN"} {
		if !strings.Contains(out, want) {
			t.Errorf("WriteText output missing %q\n---\n%s", want, out)
		}
	}
}
