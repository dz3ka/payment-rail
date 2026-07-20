package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Sign returns the Stripe-style signature header value for a webhook body:
//
//	t=<unixSeconds>,v1=<hex(HMAC_SHA256(secret, "<t>.<body>"))>
//
// The signed content is the decimal timestamp, a literal ".", then the raw body
// bytes. The prefix is written to the MAC directly and the body is Write'd as
// raw bytes, so the payload is never coerced through a %s format that could
// mangle non-UTF-8 or invalid-JSON values. A subscriber recomputes the same MAC
// over "<t>.<body>" with its shared signing secret to authenticate the request.
func Sign(secret []byte, t int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.", t)
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", t, hex.EncodeToString(mac.Sum(nil)))
}
