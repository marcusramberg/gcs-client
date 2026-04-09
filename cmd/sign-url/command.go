package signurl

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"
	"google.golang.org/api/option"

	"github.com/marcusramberg/gcs-client/pkg/utils"
)

var Command = &cli.Command{
	Name:      "sign-url",
	Usage:     "Generates a signed URL that gives unauthenticated access to a GCS resource",
	ArgsUsage: "URL [URL ...]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "account",
			Usage: "The service account email to use for signing.",
		},
		&cli.DurationFlag{
			Name:    "duration",
			Aliases: []string{"d"},
			Usage:   "The duration for which the signed URL is valid.",
			Value:   time.Hour,
		},
		&cli.StringSliceFlag{
			Name:  "headers",
			Usage: "HTTP headers to include.",
		},
		&cli.StringFlag{
			Name:  "http-verb",
			Usage: "The HTTP verb to use.",
			Value: "GET",
		},
		&cli.BoolFlag{
			Name:  "path-style-url",
			Usage: "Whether to use path-style URLs.",
		},
		&cli.StringFlag{
			Name:  "private-key-file",
			Usage: "Path to a service account private key file.",
		},
		&cli.StringSliceFlag{
			Name:  "query-params",
			Usage: "Query parameters to include.",
		},
		&cli.StringFlag{
			Name:  "region",
			Usage: "The region in which to sign the URL.",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Len() == 0 {
			return utils.ErrURLRequired
		}

		var opts []option.ClientOption
		if keyFile := cmd.String("private-key-file"); keyFile != "" {
			opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, keyFile))
		}

		client, err := utils.NewClient(ctx, opts...)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer client.Close()

		duration := cmd.Duration("duration")
		method := cmd.String("http-verb")
		headers := cmd.StringSlice("headers")
		queryParams := cmd.StringSlice("query-params")

		signOpts := &storage.SignedURLOptions{
			Method:         method,
			Expires:        time.Now().Add(duration),
			Headers:        headers,
			GoogleAccessID: cmd.String("account"),
		}

		if cmd.Bool("path-style-url") {
			signOpts.Style = storage.PathStyle()
		}

		if len(queryParams) > 0 {
			signOpts.QueryParameters = make(url.Values)
			for _, p := range queryParams {
				parts := strings.SplitN(p, "=", 2)
				if len(parts) == 2 {
					signOpts.QueryParameters.Add(parts[0], parts[1])
				} else {
					signOpts.QueryParameters.Add(parts[0], "")
				}
			}
		}

		for _, arg := range cmd.Args().Slice() {
			bucket, object, _, err := utils.ParseGCSPath(arg)
			if err != nil {
				return err
			}

			if object == "" {
				return fmt.Errorf("%w: %s", utils.ErrObjectRequired, arg)
			}

			url, err := client.Bucket(bucket).SignedURL(object, signOpts)
			if err != nil {
				return fmt.Errorf("failed to sign URL for %s: %w", arg, err)
			}
			fmt.Println(url)
		}

		return nil
	},
}
