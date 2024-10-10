package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/preendata/preen/internal/cli"
	"github.com/preendata/preen/internal/engine"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		if os.Getenv("PREEN_DEBUG") == "true" {
			log.Print("warn: error loading .env file", err)
		}
	}

	err = engine.Initialize()
	if err != nil {
		log.Print("error initializing logging", err)
	}

	app := cli.NewApp()
	if err := app.Run(os.Args); err != nil {
		engine.Fatal(err)
	}
}
