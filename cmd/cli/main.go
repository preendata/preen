package main

import (
	"log"
	"os"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/pkg/cli"
)

func main() {
	err := config.Initialize()

	if err != nil {
		log.Fatalf("Error initializing CLI: %v", err)
	}

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
