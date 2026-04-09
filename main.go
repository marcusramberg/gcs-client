package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/cmd/cat"
	"github.com/marcusramberg/gcs-client/cmd/cp"
	"github.com/marcusramberg/gcs-client/cmd/hash"
	"github.com/marcusramberg/gcs-client/cmd/ls"
	"github.com/marcusramberg/gcs-client/cmd/mv"
	"github.com/marcusramberg/gcs-client/cmd/restore"
	"github.com/marcusramberg/gcs-client/cmd/rm"
	signurl "github.com/marcusramberg/gcs-client/cmd/sign-url"
)

var version = "dev"

func main() {
	cmd := &cli.Command{
		EnableShellCompletion:  true,
		UseShortOptionHandling: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "project",
				Aliases: []string{"p"},
				Usage:   "Google Cloud Project ID",
				Sources: cli.EnvVars("GOOGLE_CLOUD_PROJECT"),
			},
		},
		Commands: []*cli.Command{
			cp.Command,
			cat.Command,
			rm.Command,
			mv.Command,
			ls.Command,
			hash.Command,
			restore.Command,
			signurl.Command,
			{
				Name: "version",
				Action: func(c context.Context, cmd *cli.Command) error {
					slog.Info("gcs-client version", "version", version)
					return nil
				},
			},
		},
	}
	timeout := 300 * time.Second
	timeoutOption := os.Getenv("GCS_CLIENT_TIMEOUT")
	if timeoutOption != "" {
		var err error
		timeout, err = time.ParseDuration(timeoutOption)
		if err != nil {
			slog.Warn("failed to parse GCS_CLIENT_TIMEOUT, using default", "error", err)
			timeout = 300 * time.Second
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if err := cmd.Run(ctx, os.Args); err != nil {
		fmt.Printf("an error occurred: %v\n", err)
	}
	cancel()
}
