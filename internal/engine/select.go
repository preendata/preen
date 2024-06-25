package engine

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

// MainParser handles coordination of actions that must be determined from the input statement and applied across
// the entire batch of queries or result sets. For example, joins require the join condition to be parsed and applied
// to all queries in the batch and can not be handled in isolation.
func (q *Query) MainParser() error {
	table := q.Main.Select.From[0]

	switch tableList := table.(type) {
	case *sqlparser.JoinTableExpr:
		q.JoinDetails.JoinExpr = tableList
		q.JoinNodeQuery()
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
