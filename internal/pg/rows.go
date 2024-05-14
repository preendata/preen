package pg

import (
	"github.com/jackc/pgx/v5/pgconn"
)

func ParseResult(results []*pgconn.Result) [][]string {
	var parsedResults [][]string
	for _, result := range results {
		for _, row := range result.Rows {
			var parsedRow []string
			for _, column := range row {
				parsedRow = append(parsedRow, string(column))
			}
			parsedResults = append(parsedResults, parsedRow)
		}
	}

	return parsedResults
}
