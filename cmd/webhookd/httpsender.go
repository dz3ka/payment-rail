package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/dz3ka/payment-rail/internal/webhook"
)

// httpSender is the net/http implementation of the webhook.Sender port — the only
// place in webhookd making outbound HTTP requests. It keeps internal/webhook free
// of net/http so the delivery logic stays transport-agnostic and unit-testable.
type httpSender struct {
	client *http.Client
}

// Compile-time proof the adapter satisfies the port.
var _ webhook.Sender = (*httpSender)(nil)

// newHTTPSender builds a client bounded by webhook.HTTPTimeout that refuses to
// follow redirects: a subscriber must not be able to bounce a signed POST to an
// unintended host, and a 3xx is surfaced to the worker's classify as a non-2xx.
func newHTTPSender() *httpSender {
	return &httpSender{client: &http.Client{
		Timeout: webhook.HTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("webhook: redirects not followed")
		},
	}}
}

// Send POSTs the signed payload to the subscriber URL and reports the HTTP status.
// A non-2xx response is NOT an error here — the worker's classify decides the
// outcome; an error is returned only when no response was received (bad scheme,
// build failure, transport error). The signing secret is never included in an
// error, and the response body is drained-and-capped purely for connection reuse.
//
// SSRF posture: only the http/https scheme allowlist and redirect refusal are
// enforced here. HTTPS-only and private-IP/DNS-rebind blocking are DEFERRED —
// subscriber URLs are operator-registered and the deployment is testnet-only.
func (s *httpSender) Send(ctx context.Context, rawURL string, body []byte, sig, eventID string, attempt int) (webhook.SendResult, error) {
	// url.Parse's error echoes the raw URL verbatim, which can carry embedded
	// userinfo credentials — never fold it into the returned error (it lands in
	// last_error and logs). Report a generic parse failure instead.
	u, err := url.Parse(rawURL)
	if err != nil {
		return webhook.SendResult{}, errors.New("webhook: subscriber url did not parse")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return webhook.SendResult{}, fmt.Errorf("webhook: unsupported url scheme %q", u.Scheme)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return webhook.SendResult{}, fmt.Errorf("webhook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhook.SignatureHeader, sig)
	req.Header.Set(webhook.EventIDHeader, eventID)
	req.Header.Set(webhook.AttemptHeader, strconv.Itoa(attempt))

	resp, err := s.client.Do(req)
	if err != nil {
		return webhook.SendResult{}, fmt.Errorf("webhook: send: %w", err)
	}
	// Drain a bounded prefix so the connection can be reused, then close. The
	// body is never stored — only the status code drives the delivery outcome.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, webhook.RespBodyCap))
	_ = resp.Body.Close()

	return webhook.SendResult{StatusCode: resp.StatusCode}, nil
}
