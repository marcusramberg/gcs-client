package rm

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "rm",
	Usage:     "Delete GCS objects or local files",
	ArgsUsage: "[TARGET ...]",
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() < 1 {
			return fmt.Errorf("%w: rm command requires at least 1 argument", utils.ErrInvalidArgs)
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
				if err := utils.RetryObject(utils.RetryBucket(client, bucket), object).Delete(ctx); err != nil {
					return fmt.Errorf("failed to delete GCS object %s: %w", arg, err)
				}
			} else {
				if err := os.Remove(arg); err != nil {
					return fmt.Errorf("failed to remove local file %s: %w", arg, err)
				}
			}
		}

		return nil
	},
}
