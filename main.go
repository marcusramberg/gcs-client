package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"

	"github.com/marcusramberg/gcs-client/cmd/cat"
	"github.com/marcusramberg/gcs-client/cmd/cp"
	"github.com/marcusramberg/gcs-client/cmd/hash"
	"github.com/marcusramberg/gcs-client/cmd/ls"
	"github.com/marcusramberg/gcs-client/cmd/mv"
	"github.com/marcusramberg/gcs-client/cmd/restore"
	"github.com/marcusramberg/gcs-client/cmd/rm"
)

func main() {
	cmd := &cli.Command{
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
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Printf("an error occurred: %v\n", err)
		os.Exit(1)
	}
}
