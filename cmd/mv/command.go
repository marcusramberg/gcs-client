package mv

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "mv",
	Usage:     "Move or rename GCS objects or local files",
	ArgsUsage: "[SOURCE ...] DESTINATION",
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() < 2 {
			return fmt.Errorf("%w: mv command requires at least 2 arguments", utils.ErrInvalidArgs)
		}

		args := cmd.Args().Slice()
		srcs := args[:len(args)-1]
		dest := args[len(args)-1]

		client, err := utils.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		for _, src := range srcs {
			if err := move(ctx, client, src, dest); err != nil {
				return err
			}
		}

		return nil
	},
}

func move(ctx context.Context, client *storage.Client, src, dest string) error {
	srcIsGCS := strings.HasPrefix(src, "gs://")
	destIsGCS := strings.HasPrefix(dest, "gs://")

	switch {
	case srcIsGCS && destIsGCS:
		return moveGCSToGCS(ctx, client, src, dest)
	case srcIsGCS:
		return moveGCSToLocal(ctx, client, src, dest)
	case destIsGCS:
		return moveLocalToGCS(ctx, client, src, dest)
	default:
		return os.Rename(src, dest)
	}
}

func moveLocalToGCS(ctx context.Context, client *storage.Client, src, dest string) error {
	bucketName, objectName, _, err := utils.ParseGCSPath(dest)
	if err != nil {
		return err
	}

	if strings.HasSuffix(dest, "/") || objectName == "" {
		objectName = path.Join(objectName, filepath.Base(src))
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	w := utils.RetryObject(utils.RetryBucket(client, bucketName), objectName).NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	return os.Remove(src)
}

func moveGCSToLocal(ctx context.Context, client *storage.Client, src, dest string) error {
	bucketName, objectName, _, err := utils.ParseGCSPath(src)
	if err != nil {
		return err
	}

	fi, err := os.Stat(dest)
	if err == nil && fi.IsDir() {
		dest = filepath.Join(dest, filepath.Base(objectName))
	}

	r, err := utils.RetryObject(utils.RetryBucket(client, bucketName), objectName).NewReader(ctx)
	if err != nil {
		return err
	}
	defer r.Close()

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return err
	}

	return utils.RetryObject(utils.RetryBucket(client, bucketName), objectName).Delete(ctx)
}

func moveGCSToGCS(ctx context.Context, client *storage.Client, src, dest string) error {
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

	srcObj := utils.RetryObject(utils.RetryBucket(client, srcBucket), srcObject)
	destObj := utils.RetryObject(utils.RetryBucket(client, destBucket), destObject)

	if _, err := destObj.CopierFrom(srcObj).Run(ctx); err != nil {
		return err
	}

	return srcObj.Delete(ctx)
}
