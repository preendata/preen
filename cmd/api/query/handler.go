package query

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/engine"
)

type HandlerRequest struct {
	Query string `json:"query"`
}

type HandlerResponse struct {
	Rows []map[string]interface{} `json:"results"`
}

func Handler(c *gin.Context) {
	var err error
	config, ok := c.MustGet("config").(*config.Config)

	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "could not get hypha config!!!!",
		})
	}

	request := HandlerRequest{}
	body, err := io.ReadAll(c.Request.Body)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
	}

	err = json.Unmarshal(body, &request)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
	}

	response := HandlerResponse{}

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
	}

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
	}

	err = engine.Execute(request.Query, config)

	//response.Rows = qr.Rows

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}
