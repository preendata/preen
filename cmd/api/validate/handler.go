package validate

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
)

type HandlerResponse struct {
	*pg.Validator
}

func Handler(c *gin.Context) {
	var err error
	response := HandlerResponse{}

	config, err := config.GetConfig()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
	}

	c.JSON(500, gin.H{"error": err.Error()})

	response.Validator, err = pg.Validate(&config)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}
