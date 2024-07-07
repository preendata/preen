package main

import (
	"log"
	"os"

	"github.com/hyphadb/hyphadb/internal/hlog"

	"github.com/hyphadb/hyphadb/pkg/cli"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Print("warn: error loading .env file", err)
	}

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		hlog.Fatal(err)
	}
}
