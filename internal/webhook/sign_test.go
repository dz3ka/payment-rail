package webhook

import "testing"

func TestSignKnownVector(t *testing.T) {
	// Pinned vector: HMAC-SHA256("whsec_test", "1700000000." + body), computed
	// independently (python hmac). If Sign's construction changes, this breaks.
	const (
		secret = "whsec_test"
		ts     = int64(1700000000)
		body   = `{"id":"evt_1"}`
		want   = "t=1700000000,v1=c89214b5b5da833daed6f0b8c5bb6bd58cea9022bd80ccc78230f3942d632925"
	)
	if got := Sign([]byte(secret), ts, []byte(body)); got != want {
		t.Fatalf("Sign = %q, want %q", got, want)
	}
}

func TestSignDifferentSecretDiffersV1(t *testing.T) {
	ts := int64(1700000000)
	body := []byte(`{"id":"evt_1"}`)
	a := Sign([]byte("whsec_a"), ts, body)
	b := Sign([]byte("whsec_b"), ts, body)
	if a == b {
		t.Fatalf("different secrets produced identical signature %q", a)
	}
}

func TestSignTamperedBodyDiffersV1(t *testing.T) {
	secret := []byte("whsec_test")
	ts := int64(1700000000)
	a := Sign(secret, ts, []byte(`{"id":"evt_1"}`))
	b := Sign(secret, ts, []byte(`{"id":"evt_2"}`))
	if a == b {
		t.Fatalf("tampered body produced identical signature %q", a)
	}
}
