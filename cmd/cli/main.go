package main

import (
	"log"
	"os"

	"github.com/hyphadb/hyphadb/pkg/cli"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("warn: error loading .env file", err)
	}

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		hlog.Fatal(err)
	}
}
