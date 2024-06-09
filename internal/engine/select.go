package engine

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func (q *Query) MainParser() error {
	table := q.Main.Select.From[0]

	if q.Main.Select.Limit != nil {
		q.Main.Limit, _ = parseLimit(q.Main.Select)
	}

	switch tableList := table.(type) {
	case *sqlparser.JoinTableExpr:
		q.JoinDetails.JoinExpr = tableList
		q.ReducerRequired = true
		q.JoinNodeQuery()
	}

	return nil
}

func (p *ParsedQuery) NodeParser(sourceIndex int, nSources int) {
	if p.Select.Limit != nil {
		p.UpdateLimit(sourceIndex, nSources)
	}
}

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

func parseLimit(s *sqlparser.Select) (*int, error) {
	sLimit := sqlparser.String(s.Limit.Rowcount)
	iLimit, err := strconv.Atoi(sLimit)

	if err != nil {
		slog.Error("Error parsing limit: %s", err)
		return nil, err
	}

	return &iLimit, err
}
