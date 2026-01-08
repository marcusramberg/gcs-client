package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/cmd/cat"
	"github.com/marcusramberg/gcs-client/cmd/cp"
	"github.com/marcusramberg/gcs-client/cmd/hash"
	"github.com/marcusramberg/gcs-client/cmd/ls"
	"github.com/marcusramberg/gcs-client/cmd/mv"
	"github.com/marcusramberg/gcs-client/cmd/restore"
	"github.com/marcusramberg/gcs-client/cmd/rm"
	"github.com/marcusramberg/gcs-client/cmd/sign-url"
)

var version = "dev"

func main() {
	cmd := &cli.Command{
		EnableShellCompletion: true,
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
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Printf("an error occurred: %v\n", err)
		os.Exit(1)
	}
}
