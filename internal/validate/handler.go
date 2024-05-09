package validate

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/scalecraft/plex-db/internal/config"
	"github.com/scalecraft/plex-db/internal/pg"
)

type HandlerResponse struct {
	*pg.Validator
}

func Handler(c *gin.Context) {
	var err error
	response := HandlerResponse{}

	config := config.GetConfig()

	response.Validator, err = pg.Validate(&config)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}
