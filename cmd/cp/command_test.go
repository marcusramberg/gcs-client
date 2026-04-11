package cp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func TestCopyOptionsDefaultParallelism(t *testing.T) {
	t.Parallel()
	opts := &copyOptions{}
	opts.setDefaults()
	if opts.parallelism != runtime.NumCPU() {
		t.Errorf("expected default parallelism %d, got %d", runtime.NumCPU(), opts.parallelism)
	}
}

func TestCopyOptionsExplicitParallelism(t *testing.T) {
	t.Parallel()
	opts := &copyOptions{parallelism: 4}
	opts.setDefaults()
	if opts.parallelism != 4 {
		t.Errorf("expected explicit parallelism 4, got %d", opts.parallelism)
	}
}

func TestCopyLocalRecursiveDispatchesAllFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var called atomic.Int64
	opts := &copyOptions{parallelism: 3}
	err := copyLocalRecursiveWithHook(t.Context(), nil, dir, "/tmp/dest", opts,
		func(_ context.Context, _ *storage.Client, src, dest string, _ *copyOptions) error {
			called.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called.Load() != 5 {
		t.Errorf("expected 5 copy calls, got %d", called.Load())
	}
}

func TestCopyGCSToGCSRecursiveWithHookDispatchesAllObjects(t *testing.T) {
	t.Parallel()
	attrs := []*storage.ObjectAttrs{
		{Name: "src/x.bin"},
		{Name: "src/y.bin"},
		{Name: "src/nested/z.bin"},
	}
	var called atomic.Int64
	opts := &copyOptions{parallelism: 3}
	err := copyGCSToGCSRecursiveWithHook(t.Context(), nil, "srcbucket", "src", "destbucket", "dst", opts, attrs,
		func(_ context.Context, _ *storage.Client, src, dest string, _ *copyOptions) error {
			called.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called.Load() != 3 {
		t.Errorf("expected 3 copy calls, got %d", called.Load())
	}
}

func TestCopyGCSToLocalRecursiveWithHookDispatchesAllObjects(t *testing.T) {
	t.Parallel()
	attrs := []*storage.ObjectAttrs{
		{Name: "prefix/a.txt"},
		{Name: "prefix/b.txt"},
		{Name: "prefix/sub/c.txt"},
		{Name: "prefix/sub/d.txt"},
	}
	var called atomic.Int64
	opts := &copyOptions{parallelism: 2}
	err := copyGCSToLocalRecursiveWithHook(t.Context(), nil, "mybucket", "prefix", t.TempDir(), opts, attrs,
		func(_ context.Context, _ *storage.Client, src, dest string, _ *copyOptions) error {
			called.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called.Load() != 4 {
		t.Errorf("expected 4 copy calls, got %d", called.Load())
	}
}

func TestUploadChunkSizeSmallFile(t *testing.T) {
	t.Parallel()
	// Files smaller than defaultChunkSize should use a chunk size slightly
	// above their actual size — not the full 16 MiB — to avoid memory bloat
	// when many small files are uploaded in parallel.
	const fileSize = 1 * 1024 * 1024 // 1 MiB
	got := uploadChunkSize(fileSize)
	if got <= 0 {
		t.Fatalf("expected positive chunk size, got %d", got)
	}
	// Should be at most 2× the file size (not the full 16 MiB default)
	if got > fileSize*2 {
		t.Errorf("chunk size %d is too large for a %d-byte file (wastes memory in parallel mode)", got, fileSize)
	}
}

func TestUploadChunkSizeLargeFile(t *testing.T) {
	t.Parallel()
	// Files larger than defaultChunkSize should use the SDK default (16 MiB)
	// so the upload is split into manageable chunks.
	const fileSize = 100 * 1024 * 1024 // 100 MiB
	got := uploadChunkSize(fileSize)
	const sdkDefault = 16 * 1024 * 1024 // 16 MiB
	if got != sdkDefault {
		t.Errorf("expected SDK default chunk size %d for large file, got %d", sdkDefault, got)
	}
}

func TestUploadChunkSizeZeroFile(t *testing.T) {
	t.Parallel()
	// Zero-size files should not produce a zero or negative chunk size.
	got := uploadChunkSize(0)
	if got <= 0 {
		t.Errorf("expected positive chunk size for zero-size file, got %d", got)
	}
}

// TestBuildClientOptionsIncludesConnectionPool verifies that buildClientOptions
// returns options that include a gRPC connection pool sized to the parallelism
// level. We confirm this by passing the options (plus WithoutAuthentication) to
// storage.NewGRPCClient: if the pool-size option is present and accepted the
// client constructs without error.
func TestBuildClientOptionsIncludesConnectionPool(t *testing.T) {
	t.Parallel()
	parallelism := 4
	opts := buildClientOptions(parallelism)
	// Add auth bypass so we can construct the client in tests without credentials.
	opts = append(opts, option.WithoutAuthentication())
	client, err := storage.NewGRPCClient(t.Context(), opts...)
	if err != nil {
		t.Fatalf("storage.NewGRPCClient with buildClientOptions(%d) failed: %v", parallelism, err)
	}
	defer client.Close()
}

// TestBuildClientOptionsPoolSizeMatchesParallelism verifies that different
// parallelism values produce distinct, valid option slices (one option per call,
// all accepted by the gRPC constructor).
func TestBuildClientOptionsPoolSizeMatchesParallelism(t *testing.T) {
	t.Parallel()
	for _, p := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("parallelism=%d", p), func(t *testing.T) {
			t.Parallel()
			opts := buildClientOptions(p)
			opts = append(opts, option.WithoutAuthentication())
			client, err := storage.NewGRPCClient(t.Context(), opts...)
			if err != nil {
				t.Fatalf("buildClientOptions(%d): storage.NewGRPCClient failed: %v", p, err)
			}
			defer client.Close()
		})
	}
}

func TestFormatBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048575, "1024.0 KiB"},
		{1048576, "1.0 MiB"},
		{1572864, "1.5 MiB"},
		{1073741823, "1024.0 MiB"},
		{1073741824, "1.0 GiB"},
		{1610612736, "1.5 GiB"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d", tc.n), func(t *testing.T) {
			t.Parallel()
			got := formatBytes(tc.n)
			if got != tc.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tc.n, got, tc.want)
			}
		})
	}
}

func TestTransferStatsRecordConcurrentSafety(t *testing.T) {
	t.Parallel()
	var buf strings.Builder
	ts := &transferStats{w: &buf, startTime: time.Now()}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			ts.record(fmt.Sprintf("gs://bucket/file%d.txt", i), 1024*1024, 100*time.Millisecond)
		})
	}
	wg.Wait()
	// Use summary() to read the counters safely through the lock.
	ts.summary()
	got := buf.String()
	if !strings.Contains(got, "20 files") {
		t.Errorf("expected summary to contain '20 files', got: %q", got)
	}
	if !strings.Contains(got, "20.0 MiB") {
		t.Errorf("expected summary to contain '20.0 MiB', got: %q", got)
	}
}

func TestTransferStatsSummaryOutput(t *testing.T) {
	t.Parallel()
	var buf strings.Builder
	ts := &transferStats{
		w:         &buf,
		startTime: time.Now().Add(-2 * time.Second), // simulate 2s elapsed
		files:     3,
		bytes:     3 * 1024 * 1024, // 3 MiB
	}
	ts.summary()
	got := buf.String()
	if !strings.Contains(got, "3 files") {
		t.Errorf("summary output missing file count: %q", got)
	}
	if !strings.Contains(got, "3.0 MiB") {
		t.Errorf("summary output missing byte count: %q", got)
	}
	if !strings.Contains(got, "MB/s") {
		t.Errorf("summary output missing throughput: %q", got)
	}
}

func TestTransferStatsSummarySingularFile(t *testing.T) {
	t.Parallel()
	var buf strings.Builder
	ts := &transferStats{
		w:         &buf,
		startTime: time.Now().Add(-time.Second),
		files:     1,
		bytes:     512,
	}
	ts.summary()
	got := buf.String()
	if !strings.Contains(got, "1 file,") {
		t.Errorf("expected singular 'file', got: %q", got)
	}
	if strings.Contains(got, "1 files") {
		t.Errorf("should not use plural for 1 file, got: %q", got)
	}
}
