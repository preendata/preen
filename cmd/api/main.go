package main

import (
	"flag"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/cmd/api/query"
	"github.com/hyphadb/hyphadb/cmd/api/stats"
	"github.com/hyphadb/hyphadb/cmd/api/validate"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()

	if err != nil {
		log.Fatalf("warn: error loading .env file: %v", err)
	}

	var verbose bool
	flag.BoolVar(&verbose, "v", false, "Set the log level to DEBUG (shorthand)")
	flag.Parse()

	logLevel := "ERROR"
	if verbose {
		logLevel = "DEBUG"
	}

	if err := hlog.Initialize(logLevel); err != nil {
		log.Fatalf("fatal error initializing logger: %v", err)
	}

	config.Initialize()
}

func middleware(config *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("config", config)
		c.Next()
	}
}

func main() {
	r := gin.Default()

	ginConfig := cors.DefaultConfig()
	ginConfig.AllowAllOrigins = true
	r.Use(cors.New(ginConfig))
	r.Use(middleware(&config.GlobalConfig))

	err := r.SetTrustedProxies(nil)

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
