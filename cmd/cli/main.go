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
		// Use builtin log before hlog instantiation
		log.Fatalf("warn: error loading .env file: %v", err)
	}

	hlog.Initialize()
	config.Initialize()

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		hlog.Fatal(err)
	}
}
