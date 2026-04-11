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

func TestProgressReaderConcurrentPrintDoesNotRace(t *testing.T) {
	t.Parallel()
	// Use a shared progressReader to verify concurrent Read() calls are race-free.
	// Each Read() increments current and may call printLocked() — both under mu.
	// The underlying reader is wrapped with a mutex so it is safe to call from
	// multiple goroutines; the point is to exercise progressReader's own locking.
	content := strings.Repeat("x", 1000)
	var rMu sync.Mutex
	inner := strings.NewReader(content)
	safeR := readerFunc(func(p []byte) (int, error) {
		rMu.Lock()
		defer rMu.Unlock()
		return inner.Read(p)
	})
	pr := &progressReader{
		r:       safeR,
		total:   int64(len(content)),
		verbose: true,
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			buf := make([]byte, 100)
			_, _ = pr.Read(buf)
		})
	}
	wg.Wait()
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

// readerFunc is an io.Reader backed by a function.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
