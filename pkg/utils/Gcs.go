// Package utils provides utility functions
package utils

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var ErrInvalidGCSPath = errors.New("invalid GCS path")

// NewClient creates a new GCS client using the gRPC transport.
// The gRPC client enables DirectPath on GCP infrastructure, routing traffic
// over Google's internal network for significantly higher throughput.
//
// gRPC client metrics are disabled by default: the SDK logs a warning when it
// cannot find a GCP project to export telemetry to Cloud Monitoring, which is
// noise for a CLI tool.  Callers that want metrics can pass their own
// meterProvider via storage options.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
	// Prepend so caller-supplied options take precedence.
	allOpts := append([]option.ClientOption{storage.WithDisabledClientMetrics()}, opts...)
	return storage.NewGRPCClient(ctx, allOpts...)
}

// RetryObject returns an ObjectHandle with RetryAlways policy.
func RetryObject(bucket *storage.BucketHandle, name string) *storage.ObjectHandle {
	return bucket.Object(name).Retryer(storage.WithPolicy(storage.RetryAlways))
}

// RetryBucket returns a BucketHandle with RetryAlways policy.
func RetryBucket(client *storage.Client, name string) *storage.BucketHandle {
	return client.Bucket(name).Retryer(storage.WithPolicy(storage.RetryAlways))
}

// ParseGCSPath parses a GCS path of the form "gs://bucket/object[#generation]" and returns the bucket, object, and generation.
func ParseGCSPath(path string) (string, string, int64, error) {
	if len(path) < 5 || path[:5] != "gs://" {
		return "", "", 0, fmt.Errorf("%w: %s", ErrInvalidGCSPath, path)
	}
	fullPath := path[5:]
	parts := strings.SplitN(fullPath, "/", 2)
	bucket := parts[0]
	if len(parts) < 2 {
		return bucket, "", 0, nil
	}
	objectWithGen := parts[1]
	objectParts := strings.SplitN(objectWithGen, "#", 2)
	object := objectParts[0]
	var generation int64
	if len(objectParts) > 1 {
		_, _ = fmt.Sscanf(objectParts[1], "%d", &generation)
	}
	return bucket, object, generation, nil
}
