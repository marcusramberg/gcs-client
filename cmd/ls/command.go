package ls

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"
	"google.golang.org/api/iterator"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "ls",
	Usage:     "List GCS buckets and objects",
	ArgsUsage: "[URL ...]",
	Action: func(ctx context.Context, cmd *cli.Command) error {
		projectID := cmd.String("project")
		client, err := utils.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		if cmd.Args().Len() == 0 {
			// List buckets
			it := client.Buckets(ctx, projectID)
			for {
				battrs, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}
				if err != nil {
					return err
				}
				fmt.Println("gs://" + battrs.Name)
			}
			return nil
		}

		for _, arg := range cmd.Args().Slice() {
			if !strings.HasPrefix(arg, "gs://") {
				return fmt.Errorf("%w: %s", utils.ErrOnlyGCS, arg)
			}
			bucket, prefix, _, err := utils.ParseGCSPath(arg)
			if err != nil {
				return err
			}

			it := utils.RetryBucket(client, bucket).Objects(ctx, &storage.Query{Prefix: prefix})
			for {
				attrs, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}
				if err != nil {
					return err
				}
				fmt.Printf("gs://%s/%s\n", attrs.Bucket, attrs.Name)
			}
		}

		return nil
	},
}
