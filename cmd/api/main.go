package main

import (
	"flag"
	"log/slog"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/cmd/api/query"
	"github.com/hyphadb/hyphadb/cmd/api/validate"
)

func main() {
	flag.Parse()
	r := gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	r.Use(cors.New(config))

	err := r.SetTrustedProxies(nil)

	if err != nil {
		slog.Error("Failed to set trusted proxies", err)
	}

	r.GET("/validate", validate.Handler)

	r.POST("/query", query.Handler)

	err = r.Run(":5051")

	if err != nil {
		slog.Error("Failed to start server", err)
	}
}
