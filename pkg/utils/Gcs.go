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

var (
	ErrInvalidGCSPath = errors.New("invalid GCS path")
)

// NewClient creates a new GCS client with robust settings.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
	finalOpts := append([]option.ClientOption{storage.WithJSONReads()}, opts...)
	return storage.NewClient(ctx, finalOpts...)
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
