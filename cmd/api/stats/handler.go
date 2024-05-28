package stats

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
)

type HandlerResponse struct {
	Stats []pg.Stats
}

func Handler(c *gin.Context) {
	var err error
	response := HandlerResponse{}

	config := config.GetConfig()

	response.Stats, err = pg.GetStats(&config)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}
