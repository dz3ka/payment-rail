package main

import (
	"strings"
	"testing"
)

// TestReplayWebhook_MissingSubscriptionIDFailsFast proves the required-flag
// guard rejects a missing --subscription-id BEFORE any config load or DB dial,
// so the command can never touch Postgres without a target. Fully hermetic.
func TestReplayWebhook_MissingSubscriptionIDFailsFast(t *testing.T) {
	err := runReplayWebhook(nil)
	if err == nil {
		t.Fatal("runReplayWebhook(nil) = nil, want an error for a missing --subscription-id")
	}
	if !strings.Contains(err.Error(), "subscription-id") {
		t.Fatalf("runReplayWebhook(nil) error = %v, want it to name --subscription-id", err)
	}
}

// TestReplayWebhook_InvalidSubscriptionIDFailsFast proves a malformed uuid is
// rejected at the parse step, before any I/O, so a bad id never reaches the DB.
func TestReplayWebhook_InvalidSubscriptionIDFailsFast(t *testing.T) {
	err := runReplayWebhook([]string{"--subscription-id", "not-a-uuid"})
	if err == nil {
		t.Fatal("runReplayWebhook() = nil, want an error for a malformed --subscription-id")
	}
	if !strings.Contains(err.Error(), "subscription-id") {
		t.Fatalf("runReplayWebhook() error = %v, want it to name --subscription-id", err)
	}
}
