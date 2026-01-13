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
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"
	"google.golang.org/api/iterator"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "cp",
	Usage:     "Copy files and objects to and from Google Cloud Storage",
	ArgsUsage: "[SOURCE ...] DESTINATION",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "recursive", Aliases: []string{"r"}, Usage: "Recursive copy"},
		&cli.BoolFlag{Name: "no-clobber", Aliases: []string{"n"}, Usage: "Do not overwrite existing files"},
		&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Show progress periodically while copying"},
		&cli.StringFlag{Name: "cache-control", Usage: "Set cache-control header"},
		&cli.StringFlag{Name: "content-type", Usage: "Set content-type header"},
		&cli.StringFlag{Name: "storage-class", Usage: "Set storage class"},
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

		client, err := utils.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		for _, src := range srcs {
			if err := performCopy(ctx, client, src, dest, opts); err != nil {
				return err
			}
		}

		return nil
	},
}

type copyOptions struct {
	recursive    bool
	noClobber    bool
	verbose      bool
	cacheControl string
	contentType  string
	storageClass string
}

type progressReader struct {
	r         io.Reader
	total     int64
	current   int64
	lastPrint time.Time
	verbose   bool
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	pr.current += int64(n)
	if pr.verbose {
		pr.print()
	}
	return n, err
}

func (pr *progressReader) print() {
	if time.Since(pr.lastPrint) < 500*time.Millisecond {
		return
	}
	pr.lastPrint = time.Now()
	percent := 0.0
	if pr.total > 0 {
		percent = float64(pr.current) / float64(pr.total) * 100
	}
	fmt.Fprintf(os.Stderr, "\rProgress: %d/%d (%.1f%%)", pr.current, pr.total, percent)
}

func performCopy(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	srcIsGCS := strings.HasPrefix(src, "gs://")
	destIsGCS := strings.HasPrefix(dest, "gs://")

	if !srcIsGCS && opts.recursive {
		if err := copyLocalRecursive(ctx, client, src, dest, opts); err != nil {
			return err
		}
	}

	switch {
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

func copyLocalRecursive(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	fi, err := os.Stat(src)
	if err != nil || !fi.IsDir() {
		return err
	}
	destIsGCS := strings.HasPrefix(dest, "gs://")

	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
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
		subDest := ""
		if destIsGCS {
			bucket, object, _, _ := utils.ParseGCSPath(dest)
			subDest = "gs://" + path.Join(bucket, object, rel)
		} else {
			subDest = filepath.Join(dest, rel)
		}
		return performCopy(ctx, client, p, subDest, opts)
	})
}

func copyLocalToGCS(ctx context.Context, client *storage.Client, src, dest string, opts *copyOptions) error {
	bucketName, objectName, _, err := utils.ParseGCSPath(dest)
	if err != nil {
		return err
	}

	if strings.HasSuffix(dest, "/") || objectName == "" {
		objectName = path.Join(objectName, filepath.Base(src))
	}

	if opts.verbose {
		fmt.Fprintf(os.Stderr, "Copying %s to gs://%s/%s\n", src, bucketName, objectName)
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
	w.ChunkSize = 0 // Use single chunk upload

	var reader io.Reader = f
	if opts.verbose {
		reader = &progressReader{r: f, total: fi.Size(), verbose: true}
	}

	if _, err := io.Copy(w, reader); err != nil {
		w.Close()
		return fmt.Errorf("failed to copy to GCS: %w", err)
	}
	if opts.verbose {
		fmt.Fprintln(os.Stderr)
	}

	return w.Close()
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

	if opts.verbose {
		fmt.Fprintf(os.Stderr, "Copying gs://%s/%s to %s\n", bucketName, objectName, dest)
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

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer f.Close()

	var reader io.Reader = r
	if opts.verbose {
		reader = &progressReader{r: r, total: r.Attrs.Size, verbose: true}
	}

	if _, err := io.Copy(f, reader); err != nil {
		return fmt.Errorf("failed to copy from GCS: %w", err)
	}
	if opts.verbose {
		fmt.Fprintln(os.Stderr)
	}

	return nil
}

func copyGCSToLocalRecursive(ctx context.Context, client *storage.Client, bucketName, objectName, dest string, opts *copyOptions) error {
	it := utils.RetryBucket(client, bucketName).Objects(ctx, &storage.Query{Prefix: objectName})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(objectName, attrs.Name)
		if err != nil {
			rel = filepath.Base(attrs.Name)
		}
		subDest := filepath.Join(dest, rel)
		if err := os.MkdirAll(filepath.Dir(subDest), 0o755); err != nil {
			return err
		}
		if err := copyGCSToLocal(ctx, client, "gs://"+path.Join(bucketName, attrs.Name), subDest, opts); err != nil {
			return err
		}
	}
	return nil
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

	if opts.verbose {
		fmt.Fprintf(os.Stderr, "Copying gs://%s/%s to gs://%s/%s\n", srcBucket, srcObject, destBucket, destObject)
	}

	if opts.noClobber {
		if _, err := utils.RetryObject(utils.RetryBucket(client, destBucket), destObject).Attrs(ctx); err == nil {
			return nil // Skip
		}
	}

	srcObj := utils.RetryObject(utils.RetryBucket(client, srcBucket), srcObject)
	destObj := utils.RetryObject(utils.RetryBucket(client, destBucket), destObject)

	copier := destObj.CopierFrom(srcObj)
	if opts.verbose {
		lastPrint := time.Now()
		copier.ProgressFunc = func(copied, total uint64) {
			if time.Since(lastPrint) < 500*time.Millisecond {
				return
			}
			lastPrint = time.Now()
			percent := 0.0
			if total > 0 {
				percent = float64(copied) / float64(total) * 100
			}
			fmt.Fprintf(os.Stderr, "\rProgress: %d/%d (%.1f%%)", copied, total, percent)
		}
	}

	if _, err := copier.Run(ctx); err != nil {
		if opts.recursive {
			return copyGCSToGCSRecursive(ctx, client, srcBucket, srcObject, destBucket, destObject, opts)
		}
		return fmt.Errorf("failed to copy GCS to GCS: %w", err)
	}
	if opts.verbose {
		fmt.Fprintln(os.Stderr)
	}

	return nil
}

func copyGCSToGCSRecursive(ctx context.Context, client *storage.Client, srcBucket, srcObject, destBucket, destObject string, opts *copyOptions) error {
	it := utils.RetryBucket(client, srcBucket).Objects(ctx, &storage.Query{Prefix: srcObject})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcObject, attrs.Name)
		if err != nil {
			rel = filepath.Base(attrs.Name)
		}
		subDest := "gs://" + path.Join(destBucket, destObject, rel)
		if err := copyGCSToGCS(ctx, client, "gs://"+path.Join(srcBucket, attrs.Name), subDest, opts); err != nil {
			return err
		}
	}
	return nil
}

func copyLocalToLocal(src, dest string, opts *copyOptions) error {
	fi, err := os.Stat(dest)
	if err == nil && fi.IsDir() {
		dest = filepath.Join(dest, filepath.Base(src))
	}

	if opts.verbose {
		fmt.Fprintf(os.Stderr, "Copying %s to %s\n", src, dest)
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

	sfi, err := sf.Stat()
	if err != nil {
		return err
	}

	df, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer df.Close()

	var reader io.Reader = sf
	if opts.verbose {
		reader = &progressReader{r: sf, total: sfi.Size(), verbose: true}
	}

	_, err = io.Copy(df, reader)
	if opts.verbose {
		fmt.Fprintln(os.Stderr)
	}
	return err
}
