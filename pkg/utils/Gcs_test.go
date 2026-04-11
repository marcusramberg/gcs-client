package utils

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// TestNewClientUsesGRPC verifies that NewClient uses the gRPC transport by
// confirming it returns an error when storage.WithJSONReads() is passed — an
// option that is incompatible with gRPC (storage.NewGRPCClient rejects it).
// If NewClient were using storage.NewClient (HTTP), it would silently accept
// WithJSONReads and this test would fail.
func TestNewClientUsesGRPC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := NewClient(ctx,
		storage.WithJSONReads(), // gRPC-incompatible option
		option.WithoutAuthentication(),
	)
	if err == nil {
		t.Fatal("expected NewClient to reject WithJSONReads (gRPC transport is incompatible with JSON reads option), but got nil error — is NewClient still using the HTTP transport?")
	}
}

// TestNewClientSucceedsWithoutJSONReads verifies that NewClient constructs
// successfully when no gRPC-incompatible options are passed.
func TestNewClientSucceedsWithoutJSONReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient failed without incompatible options: %v", err)
	}
	defer client.Close()
}
