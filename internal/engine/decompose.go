package engine

import (
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/xwb1989/sqlparser"
)

type DecomposedQuery struct {
	Statement sqlparser.Statement
	Select    *sqlparser.Select
	// A single input can be decomposed into multiple queries
	QueryStrings   []string
	Source         config.Source
	Columns        map[string]Column
	OrderedColumns []string
	Limit          *int
}

func Decompose(s *sqlparser.Select) error {
	table := s.From[0]

	switch tableList := table.(type) {
	case *sqlparser.JoinTableExpr:
		joinExpr = tableList
		decomposeJoinQuery()
	}

	return nil
}

func (p *ParsedQuery) NodeParser(sourceIndex int, nSources int) {
	p.ParseColumns()

	if p.Select.Limit != nil {
		p.UpdateLimit(sourceIndex, nSources)
	}
}

// UpdateLimit spreads the limit across every source to
// 1) Select limited data equally from all sources instead of selecting the full limited set from a single source
// 2) Ensure that the total number of rows returned is equal to the limit by adding the remainder to the first source
// UNSUPPORTED: This logic will break down if some tables have fewer rows than their portion of the limit
func (p *ParsedQuery) UpdateLimit(sourceIndex int, nSources int) {
	p.Limit, _ = parseLimit(p.Select)

	baseRowCount := int(math.Floor(float64(*p.Limit) / float64(nSources)))
	remainder := *p.Limit % nSources

	if sourceIndex == 0 {
		p.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount+remainder)))
	} else if baseRowCount > 0 {
		p.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount)))
	} else {
		p.Statement = nil
	}
}

// parseLimit parses the limit clause of the inputted select statement and
// returns the limit as an integer
func parseLimit(s *sqlparser.Select) (*int, error) {
	sLimit := sqlparser.String(s.Limit.Rowcount)
	iLimit, err := strconv.Atoi(sLimit)

	if err != nil {
		slog.Error("Error parsing limit: %s", err)
		return nil, err
	}

	return &iLimit, err
}

func decomposeJoinQuery(j *sqlparser.JoinTableExpr) error {
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
			utils.Debug(fmt.Sprintf("table %s does not exist in the config", tableName))
		}
	}

	for idx := range q.Nodes {
		for table, query := range queries {
			q.Nodes[idx].QueryString = append(q.Nodes[idx].QueryString, fmt.Sprintf(baseQuery, query, table))
		}
	}

	return nil
}
