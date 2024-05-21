package engine

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func (q *ParsedQuery) SelectParser(sourceIndex int, nSources int) *ParsedQuery {
	table := q.Select.From[0]

	switch tableList := table.(type) {
	case *sqlparser.JoinTableExpr:
		q.JoinExpr = tableList
	}
	if q.Select.Limit.Rowcount != nil {
		q.UpdateLimit(sourceIndex, nSources)
	}

	return q
}

func (q *ParsedQuery) UpdateLimit(sourceIndex int, nSources int) {
	sLimit := sqlparser.String(q.Select.Limit.Rowcount)
	iLimit, err := strconv.Atoi(sLimit)

	if err != nil {
		slog.Error("Error parsing limit: %s", err)
	}

	baseRowCount := int(math.Floor(float64(iLimit) / float64(nSources)))
	remainder := iLimit % nSources

	if sourceIndex == 0 {
		q.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount+remainder)))
	} else if baseRowCount > 0 {
		q.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount)))
	} else {
		q.Select = nil
	}
}
