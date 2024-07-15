package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/hyphadb/hyphadb/internal/hlog"
)

func (q *Query) JoinNodeQuery() error {
	join := q.JoinDetails.JoinExpr
	_, err := q.ParseJoinColumns(join)

	if err != nil {
		return err
	}

	baseQuery := "select %s from %s;"

	sourceTables := make([]string, 0)
	queries := make(map[string]string, 0)

	for _, table := range q.Cfg.Tables {
		sourceTables = append(sourceTables, table.Name)
	}

	for key := range q.Main.Columns {
		split := strings.Split(key, ".")
		tableName := split[0]
		columnName := split[1]
		if slices.Contains(sourceTables, tableName) {
			if colString, ok := queries[tableName]; !ok {
				queries[tableName] = columnName
			} else {
				queries[tableName] = colString + ", " + columnName
			}
		} else {
			hlog.Debug(fmt.Sprintf("table %s does not exist in the config", tableName))
		}
	}

	for idx := range q.Nodes {
		for table, query := range queries {
			q.Nodes[idx].QueryString = append(q.Nodes[idx].QueryString, fmt.Sprintf(baseQuery, query, table))
		}
	}

	return nil
}
