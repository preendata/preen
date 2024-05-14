package query

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
)

type HandlerRequest struct {
	Query string `json:"query"`
}

type HandlerResponse struct {
	Rows []map[string]interface{} `json:"results"`
}

func Handler(c *gin.Context) {
	var err error
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

	config := config.GetConfig()
	parsedQuery, err := ParseQuery(request.Query, &config)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
	}

	response.Rows, err = pg.Execute(parsedQuery, &config)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}
