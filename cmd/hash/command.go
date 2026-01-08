package hash

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "hash",
	Usage:     "Calculate or retrieve hashes for GCS objects or local files",
	ArgsUsage: "[TARGET ...]",
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() < 1 {
			return fmt.Errorf("%w: hash command requires at least 1 argument", utils.ErrInvalidArgs)
		}

		client, err := utils.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		for _, arg := range cmd.Args().Slice() {
			if strings.HasPrefix(arg, "gs://") {
				if err := hashGCS(ctx, client, arg); err != nil {
					return err
				}
			} else {
				if err := hashLocal(arg); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

func hashLocal(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	m := md5.New() //nolint:gosec
	c := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	mw := io.MultiWriter(m, c)
	if _, err := io.Copy(mw, f); err != nil {
		return err
	}

	fmt.Printf("Hashes for %s:\n", path)
	fmt.Printf("  CRC32C: %08x\n", c.Sum32())
	fmt.Printf("  MD5:    %s\n", base64.StdEncoding.EncodeToString(m.Sum(nil)))
	return nil
}

func hashGCS(ctx context.Context, client *storage.Client, path string) error {
	bucket, object, _, err := utils.ParseGCSPath(path)
	if err != nil {
		return err
	}

	attrs, err := utils.RetryObject(utils.RetryBucket(client, bucket), object).Attrs(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Hashes for %s:\n", path)
	fmt.Printf("  CRC32C: %08x\n", attrs.CRC32C)
	fmt.Printf("  MD5:    %s\n", base64.StdEncoding.EncodeToString(attrs.MD5))
	return nil
}
