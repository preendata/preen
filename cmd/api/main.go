package main

import (
	"flag"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/cmd/api/query"
	"github.com/hyphadb/hyphadb/cmd/api/stats"
	"github.com/hyphadb/hyphadb/cmd/api/validate"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/joho/godotenv"
)

func main() {
	// Load env
	err := godotenv.Load()
	if err != nil {
		// Use builtin log before hlog instantiation
		log.Fatalf("warn: error loading .env file: %v", err)
	}

	// Load flags
	var verbose bool
	flag.BoolVar(&verbose, "v", false, "Set the log level to DEBUG (shorthand)")
	flag.Parse()

	// Set up logging
	logLevel := "ERROR"
	if verbose {
		logLevel = "DEBUG"
	}

	if err := hlog.Initialize(logLevel); err != nil {
		log.Fatalf("fatal error initializing logger: %v", err)
	}

	// Set up server
	r := gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	r.Use(cors.New(config))

	err = r.SetTrustedProxies(nil)

	if err != nil {
		hlog.Error("Failed to set trusted proxies", err)
	}

	r.GET("/stats", stats.Handler)
	r.GET("/validate", validate.Handler)
	r.POST("/query", query.Handler)

	err = r.Run(":5051")

	if err != nil {
		hlog.Error("Failed to start server", err)
	}
}
