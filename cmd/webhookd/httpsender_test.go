package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/dz3ka/payment-rail/internal/webhook"
)

// TestHTTPSenderRejectsNonHTTPScheme proves the scheme allowlist blocks a request
// before any network call: a non-http(s) URL returns an error and never dials.
func TestHTTPSenderRejectsNonHTTPScheme(t *testing.T) {
	s := newHTTPSender()
	for _, url := range []string{"ftp://example.com/hook", "file:///etc/passwd", "://nope", "gopher://x"} {
		res, err := s.Send(context.Background(), url, []byte("{}"), "sig", "evt", 1)
		if err == nil {
			t.Errorf("Send(%q) = %+v, nil; want error", url, res)
		}
	}
}

// TestHTTPSenderPostsAndReportsStatus checks the happy path against a hermetic
// httptest server: headers are set and the returned status is the server's, with a
// non-2xx surfaced as a status (not an error) for the worker's classify.
func TestHTTPSenderPostsAndReportsStatus(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %q; want POST", r.Method)
		}
		if got := r.Header.Get(webhook.SignatureHeader); got != "sig-value" {
			t.Errorf("%s = %q; want sig-value", webhook.SignatureHeader, got)
		}
		if got := r.Header.Get(webhook.EventIDHeader); got != "evt-1" {
			t.Errorf("%s = %q; want evt-1", webhook.EventIDHeader, got)
		}
		if got := r.Header.Get(webhook.AttemptHeader); got != "3" {
			t.Errorf("%s = %q; want 3", webhook.AttemptHeader, got)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	res, err := newHTTPSender().Send(context.Background(), srv.URL, []byte(`{"a":1}`), "sig-value", "evt-1", 3)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("StatusCode = %d; want %d", res.StatusCode, http.StatusServiceUnavailable)
	}
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Errorf("server hits = %d; want 1", n)
	}
}
