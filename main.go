package main

import (
	"log"
	"os"

	"github.com/hyphadb/hyphadb/internal/utils"

	"github.com/hyphadb/hyphadb/internal/cli"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Print("warn: error loading .env file", err)
	}

	err = utils.Initialize()
	if err != nil {
		log.Print("error initializing logging", err)
	}

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		utils.Fatal(err)
	}
}
