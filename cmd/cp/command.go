// Package cp implements the 'cp' command for copying files to Google Cloud Storage.
package cp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "cp",
	Usage:     "Copy files and objects to and from Google Cloud Storage",
	ArgsUsage: "[SOURCE ...] DESTINATION",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "recursive", Aliases: []string{"r"}, Usage: "Recursive copy"},
		&cli.BoolFlag{Name: "no-clobber", Aliases: []string{"n"}, Usage: "Do not overwrite existing files"},
		&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Output progress per file and a summary"},
		&cli.StringFlag{Name: "cache-control", Usage: "Set cache-control header"},
		&cli.StringFlag{Name: "content-type", Usage: "Set content-type header"},
		&cli.StringFlag{Name: "storage-class", Usage: "Set storage class"},
		&cli.IntFlag{
			Name:        "parallelism",
			Aliases:     []string{"j"},
			Usage:       "Number of parallel copy workers",
			Value:       0,
			DefaultText: "number of CPU cores",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() < 2 {
			return fmt.Errorf("%w: cp command requires at least 2 arguments", utils.ErrInvalidArgs)
		}

		args := cmd.Args().Slice()
		srcs := args[:len(args)-1]
		dest := args[len(args)-1]

		opts := &copyOptions{
			recursive:    cmd.Bool("recursive"),
			noClobber:    cmd.Bool("no-clobber"),
			verbose:      cmd.Bool("verbose"),
			cacheControl: cmd.String("cache-control"),
			contentType:  cmd.String("content-type"),
			storageClass: cmd.String("storage-class"),
		}
		opts.parallelism = cmd.Int("parallelism")
		opts.setDefaults()

		if opts.verbose {
			opts.stats = &transferStats{w: os.Stderr, startTime: time.Now()}
		}

		client, err := utils.NewClient(ctx, buildClientOptions(opts.parallelism)...)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		for _, src := range srcs {
			if err := performCopy(ctx, client, src, dest, opts); err != nil {
				return err
			}
		}

		if opts.stats != nil {
			opts.stats.summary()
		}
		return nil
	},
}

// buildClientOptions returns the ClientOption slice for constructing a GCS
// client with the given parallelism level.  A gRPC connection pool sized to
// the parallelism level is included so that each concurrent upload goroutine
// can use a dedicated TCP connection instead of contending on a single
// multiplexed gRPC stream.
func buildClientOptions(parallelism int) []option.ClientOption {
	return []option.ClientOption{
		option.WithGRPCConnectionPool(parallelism),
		storage.WithDisabledClientMetrics(),
	}
}

type copyOptions struct {
	recursive    bool
	noClobber    bool
	verbose      bool
	cacheControl string
	contentType  string
	storageClass string
	parallelism  int
	stats        *transferStats
}

func (o *copyOptions) setDefaults() {
	if o.parallelism <= 0 {
		o.parallelism = runtime.NumCPU()
	}
}

func performCopy(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	srcIsGCS := strings.HasPrefix(src, "gs://")
	destIsGCS := strings.HasPrefix(dest, "gs://")

	switch {
	case !srcIsGCS && opts.recursive:
		return copyLocalRecursive(ctx, client, src, dest, opts)
	case srcIsGCS && destIsGCS:
		return copyGCSToGCS(ctx, client, src, dest, opts)
	case srcIsGCS:
		return copyGCSToLocal(ctx, client, src, dest, opts)
	case destIsGCS:
		return copyLocalToGCS(ctx, client, src, dest, opts)
	default:
		return copyLocalToLocal(src, dest, opts)
	}
}

type localCopyFn func(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error

func copyLocalRecursive(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	return copyLocalRecursiveWithHook(ctx, client, src, dest, opts, func(ctx context.Context, cl *storage.Client, s, d string, o *copyOptions) error {
		if strings.HasPrefix(d, "gs://") {
			return copyLocalToGCS(ctx, cl, s, d, o)
		}
		return copyLocalToLocal(s, d, o)
	})
}

func copyLocalRecursiveWithHook(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions, copyFn localCopyFn) error {
	fi, err := os.Stat(src)
	if err != nil || !fi.IsDir() {
		return err
	}
	destIsGCS := strings.HasPrefix(dest, "gs://")

	type job struct{ src, dest string }
	var jobs []job

	err = filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(filepath.Dir(src), p)
		if err != nil {
			return err
		}
		var subDest string
		if destIsGCS {
			bucket, object, _, _ := utils.ParseGCSPath(dest)
			subDest = "gs://" + path.Join(bucket, object, rel)
		} else {
			subDest = filepath.Join(dest, rel)
		}
		jobs = append(jobs, job{p, subDest})
		return nil
	})
	if err != nil {
		return err
	}

	return runPool(ctx, opts.parallelism, jobs, func(ctx context.Context, j job) error {
		return copyFn(ctx, client, j.src, j.dest, opts)
	})
}

// uploadChunkSize returns an appropriate resumable upload chunk size for a
// file of the given byte size. For small files (below the SDK 16 MiB default)
// we cap the chunk size slightly above the file size to avoid allocating a
// full 16 MiB buffer per goroutine when many small files are uploaded in
// parallel. For large files we use the SDK default so the upload is split
// into sensible 16 MiB chunks.
func uploadChunkSize(fileSize int64) int {
	const sdkDefault = 16 * 1024 * 1024 // 16 MiB — matches storage.Writer default
	const minChunk = 256 * 1024         // 256 KiB — GCS requires multiples of this

	if fileSize >= sdkDefault {
		return sdkDefault
	}
	// Round up to the next 256 KiB boundary above the file size.
	chunks := max((fileSize+int64(minChunk)-1)/int64(minChunk), 1)
	return int(chunks * int64(minChunk))
}

// formatBytes returns a human-readable representation of n bytes.
func formatBytes(n int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.1f GiB", float64(n)/float64(gib))
	case n >= mib:
		return fmt.Sprintf("%.1f MiB", float64(n)/float64(mib))
	case n >= kib:
		return fmt.Sprintf("%.1f KiB", float64(n)/float64(kib))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// transferStats collects per-transfer results and prints CI-friendly output.
// All methods are safe to call concurrently from multiple goroutines.
type transferStats struct {
	mu        sync.Mutex
	files     int64
	bytes     int64
	startTime time.Time
	w         io.Writer // stderr by default; override in tests
}

// record prints a completion line for one transfer and updates totals.
// dst is the destination path (GCS URI or local path).
// n is the number of bytes transferred. elapsed is the per-file wall time.
func (ts *transferStats) record(dst string, n int64, elapsed time.Duration) {
	mbps := 0.0
	if elapsed > 0 {
		mbps = float64(n) / elapsed.Seconds() / 1e6
	}
	line := fmt.Sprintf("Copied %s (%s, %.1f MB/s)\n", dst, formatBytes(n), mbps)
	ts.mu.Lock()
	ts.files++
	ts.bytes += n
	fmt.Fprint(ts.w, line)
	ts.mu.Unlock()
}

// summary prints the aggregate transfer summary to ts.w.
// Call once after all transfers complete.
func (ts *transferStats) summary() {
	ts.mu.Lock()
	files := ts.files
	bytes := ts.bytes
	ts.mu.Unlock()

	elapsed := time.Since(ts.startTime)
	mbps := 0.0
	if elapsed > 0 {
		mbps = float64(bytes) / elapsed.Seconds() / 1e6
	}
	fileWord := "files"
	if files == 1 {
		fileWord = "file"
	}
	// summary() is called after all transfers complete, so no concurrent
	// writes are expected; no lock needed for the write here.
	fmt.Fprintf(ts.w, "\nOperation completed: %d %s, %s transferred in %.1fs (%.1f MB/s)\n",
		files, fileWord, formatBytes(bytes), elapsed.Seconds(), mbps)
}

func copyLocalToGCS(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	bucketName, objectName, _, err := utils.ParseGCSPath(dest)
	if err != nil {
		return err
	}

	if strings.HasSuffix(dest, "/") || objectName == "" {
		objectName = path.Join(objectName, filepath.Base(src))
	}

	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open local source: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat local source: %w", err)
	}

	obj := utils.RetryObject(utils.RetryBucket(client, bucketName), objectName)
	if opts.noClobber {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}

	w := obj.NewWriter(ctx)

	if opts.cacheControl != "" {
		w.CacheControl = opts.cacheControl
	}
	if opts.contentType != "" {
		w.ContentType = opts.contentType
	}
	if opts.storageClass != "" {
		w.StorageClass = opts.storageClass
	}
	w.ChunkSize = uploadChunkSize(fi.Size())

	start := time.Now()
	n, err := io.Copy(w, f)
	if err != nil {
		w.Close()
		return fmt.Errorf("failed to copy to GCS: %w", err)
	}
	if err := w.Close(); err != nil {
		return err
	}
	if opts.stats != nil {
		opts.stats.record(fmt.Sprintf("gs://%s/%s", bucketName, objectName), n, time.Since(start))
	}
	return nil
}

func copyGCSToLocal(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	bucketName, objectName, _, err := utils.ParseGCSPath(src)
	if err != nil {
		return err
	}

	fi, err := os.Stat(dest)
	if err == nil && fi.IsDir() {
		dest = filepath.Join(dest, filepath.Base(objectName))
	}

	if opts.noClobber {
		if _, err := os.Stat(dest); err == nil {
			return nil // Skip
		}
	}

	r, err := utils.RetryObject(utils.RetryBucket(client, bucketName), objectName).NewReader(ctx)
	if err != nil {
		if opts.recursive {
			return copyGCSToLocalRecursive(ctx, client, bucketName, objectName, dest, opts)
		}
		return fmt.Errorf("failed to create GCS reader: %w", err)
	}
	defer r.Close()

	objectSize := r.Attrs.Size

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer f.Close()

	start := time.Now()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("failed to copy from GCS: %w", err)
	}
	if opts.stats != nil {
		opts.stats.record(dest, objectSize, time.Since(start))
	}
	return nil
}

func copyGCSToLocalRecursive(ctx context.Context, client *storage.Client, bucketName, objectName, dest string, opts *copyOptions) error {
	it := utils.RetryBucket(client, bucketName).Objects(ctx, &storage.Query{Prefix: objectName})
	var allAttrs []*storage.ObjectAttrs
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		allAttrs = append(allAttrs, attrs)
	}
	return copyGCSToLocalRecursiveWithHook(ctx, client, bucketName, objectName, dest, opts, allAttrs, copyGCSToLocal)
}

func copyGCSToLocalRecursiveWithHook(ctx context.Context, client *storage.Client, bucketName, objectName, dest string, opts *copyOptions, allAttrs []*storage.ObjectAttrs, copyFn localCopyFn) error {
	type job struct{ src, dest string }
	jobs := make([]job, 0, len(allAttrs))
	for _, attrs := range allAttrs {
		rel, err := filepath.Rel(objectName, attrs.Name)
		if err != nil {
			rel = filepath.Base(attrs.Name)
		}
		subDest := filepath.Join(dest, rel)
		if err := os.MkdirAll(filepath.Dir(subDest), 0o755); err != nil {
			return err
		}
		jobs = append(jobs, job{"gs://" + path.Join(bucketName, attrs.Name), subDest})
	}

	return runPool(ctx, opts.parallelism, jobs, func(ctx context.Context, j job) error {
		return copyFn(ctx, client, j.src, j.dest, opts)
	})
}

func copyGCSToGCS(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	srcBucket, srcObject, _, err := utils.ParseGCSPath(src)
	if err != nil {
		return err
	}
	destBucket, destObject, _, err := utils.ParseGCSPath(dest)
	if err != nil {
		return err
	}

	if strings.HasSuffix(dest, "/") || destObject == "" {
		destObject = path.Join(destObject, filepath.Base(srcObject))
	}

	if opts.noClobber {
		if _, err := utils.RetryObject(utils.RetryBucket(client, destBucket), destObject).Attrs(ctx); err == nil {
			return nil // Skip
		}
	}

	srcObj := utils.RetryObject(utils.RetryBucket(client, srcBucket), srcObject)
	destObj := utils.RetryObject(utils.RetryBucket(client, destBucket), destObject)

	// Fetch size before copy so we can report bytes transferred.
	var objectSize int64
	if attrs, err := srcObj.Attrs(ctx); err == nil {
		objectSize = attrs.Size
	}

	copier := destObj.CopierFrom(srcObj)

	start := time.Now()
	if _, err := copier.Run(ctx); err != nil {
		if opts.recursive {
			return copyGCSToGCSRecursive(ctx, client, srcBucket, srcObject, destBucket, destObject, opts)
		}
		return fmt.Errorf("failed to copy GCS to GCS: %w", err)
	}
	if opts.stats != nil {
		opts.stats.record(fmt.Sprintf("gs://%s/%s", destBucket, destObject), objectSize, time.Since(start))
	}
	return nil
}

func copyGCSToGCSRecursive(ctx context.Context, client *storage.Client, srcBucket, srcObject, destBucket, destObject string, opts *copyOptions) error {
	it := utils.RetryBucket(client, srcBucket).Objects(ctx, &storage.Query{Prefix: srcObject})
	var allAttrs []*storage.ObjectAttrs
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		allAttrs = append(allAttrs, attrs)
	}
	return copyGCSToGCSRecursiveWithHook(ctx, client, srcBucket, srcObject, destBucket, destObject, opts, allAttrs, copyGCSToGCS)
}

func copyGCSToGCSRecursiveWithHook(ctx context.Context, client *storage.Client, srcBucket, srcObject, destBucket, destObject string, opts *copyOptions, allAttrs []*storage.ObjectAttrs, copyFn localCopyFn) error {
	type job struct{ src, dest string }
	jobs := make([]job, 0, len(allAttrs))
	for _, attrs := range allAttrs {
		rel, err := filepath.Rel(srcObject, attrs.Name)
		if err != nil {
			rel = filepath.Base(attrs.Name)
		}
		subDest := "gs://" + path.Join(destBucket, destObject, rel)
		jobs = append(jobs, job{"gs://" + path.Join(srcBucket, attrs.Name), subDest})
	}

	return runPool(ctx, opts.parallelism, jobs, func(ctx context.Context, j job) error {
		return copyFn(ctx, client, j.src, j.dest, opts)
	})
}

func copyLocalToLocal(src, dest string, opts *copyOptions) error {
	fi, err := os.Stat(dest)
	if err == nil && fi.IsDir() {
		dest = filepath.Join(dest, filepath.Base(src))
	}

	if opts.noClobber {
		if _, err := os.Stat(dest); err == nil {
			return nil
		}
	}

	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()

	df, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer df.Close()

	start := time.Now()
	n, err := io.Copy(df, sf)
	if err != nil {
		return err
	}
	if opts.stats != nil {
		opts.stats.record(dest, n, time.Since(start))
	}
	return nil
}
