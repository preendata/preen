package main

import (
	"log"
	"os"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/pkg/cli"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("warn: error loading .env file", err)
	}

	if err = hlog.Initialize(); err != nil {
		log.Fatal("fatal error initializing logger", err)
	}

	if err = config.Initialize(); err != nil {
		hlog.WithError(err).Fatal("fatal error initializing config")
	}

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		hlog.Fatal(err)
	}
}
