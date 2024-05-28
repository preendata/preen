package main

import (
	"flag"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/cmd/api/query"
	"github.com/hyphadb/hyphadb/cmd/api/validate"
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

	flag.Parse()
	r := gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	r.Use(cors.New(config))

	err = r.SetTrustedProxies(nil)

	if err != nil {
		hlog.Error("Failed to set trusted proxies", err)
	}

	r.GET("/validate", validate.Handler)

	r.POST("/query", query.Handler)

	err = r.Run(":5051")

	if err != nil {
		hlog.Error("Failed to start server", err)
	}
}
