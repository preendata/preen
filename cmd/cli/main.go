package main

import (
	"fmt"
	"os"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/pkg/cli"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Errorf("warn: error loading .env file: %v", err)
	}

	hlog.Initialize()
	hlog.Info("test hlog exec")

	config.Initialize()

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		hlog.Fatal(err)
	}
}
