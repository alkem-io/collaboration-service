package http

import (
	"context"
	"testing"
)

// TestGetRequestIDReturnsEmptyWhenUnset covers the miss branch.
//
// The value is read out of a context by an unexported key, so the two ways to get
// nothing back — no value at all, and a value of the wrong type under the same
// key — must both yield "" rather than panicking on the type assertion. A logger
// that panicked here would take down the request it was trying to describe.
func TestGetRequestIDReturnsEmptyWhenUnset(t *testing.T) {
	if got := GetRequestID(context.Background()); got != "" {
		t.Fatalf("GetRequestID on a bare context = %q, want empty", got)
	}
	// Wrong type under the same key: the comma-ok assertion must absorb it.
	ctx := context.WithValue(context.Background(), ctxKeyRequestID, 42)
	if got := GetRequestID(ctx); got != "" {
		t.Fatalf("GetRequestID with a non-string value = %q, want empty", got)
	}
}

// TestGetRequestIDReturnsTheStoredID covers the hit branch, so the test above
// cannot pass by the function simply always returning "".
func TestGetRequestIDReturnsTheStoredID(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKeyRequestID, "req-abc")
	if got := GetRequestID(ctx); got != "req-abc" {
		t.Fatalf("GetRequestID = %q, want req-abc", got)
	}
}
