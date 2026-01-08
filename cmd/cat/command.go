package cat

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "cat",
	Usage:     "Print the content of GCS objects or local files",
	ArgsUsage: "[SOURCE ...]",
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() < 1 {
			return fmt.Errorf("%w: cat command requires at least 1 argument", utils.ErrInvalidArgs)
		}

		client, err := utils.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		for _, arg := range cmd.Args().Slice() {
			if strings.HasPrefix(arg, "gs://") {
				bucket, object, _, err := utils.ParseGCSPath(arg)
				if err != nil {
					return err
				}
				r, err := utils.RetryObject(utils.RetryBucket(client, bucket), object).NewReader(ctx)
				if err != nil {
					return fmt.Errorf("failed to create GCS reader for %s: %w", arg, err)
				}
				if _, err := io.Copy(os.Stdout, r); err != nil {
					r.Close()
					return fmt.Errorf("failed to read from GCS %s: %w", arg, err)
				}
				r.Close()
			} else {
				f, err := os.Open(arg)
				if err != nil {
					return fmt.Errorf("failed to open local file %s: %w", arg, err)
				}
				if _, err := io.Copy(os.Stdout, f); err != nil {
					f.Close()
					return fmt.Errorf("failed to read local file %s: %w", arg, err)
				}
				f.Close()
			}
		}

		return nil
	},
}
