package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "restore",
	Usage:     "Restore soft-deleted GCS objects",
	ArgsUsage: "[URL ...]",
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() < 1 {
			return fmt.Errorf("%w: restore command requires at least 1 argument (gs://bucket/object#generation)", utils.ErrInvalidArgs)
		}

		client, err := utils.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		for _, arg := range cmd.Args().Slice() {
			if !strings.HasPrefix(arg, "gs://") {
				return fmt.Errorf("%w: %s", utils.ErrOnlyGCS, arg)
			}
			bucket, object, generation, err := utils.ParseGCSPath(arg)
			if err != nil {
				return err
			}
			if generation == 0 {
				return fmt.Errorf("%w: %s", utils.ErrGenerationRequired, arg)
			}

			_, err = utils.RetryObject(utils.RetryBucket(client, bucket), object).Generation(generation).Restore(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to restore %s: %w", arg, err)
			}
			fmt.Printf("Successfully restored %s\n", arg)
		}

		return nil
	},
}
