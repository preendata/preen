package engine

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func (p *ParsedQuery) SelectParser(sourceIndex int, nSources int) {
	table := p.Select.From[0]

	switch tableList := table.(type) {
	case *sqlparser.JoinTableExpr:
		p.JoinDetails.JoinExpr = tableList
	}
	if p.Select.Limit != nil {
		p.UpdateLimit(sourceIndex, nSources)
	}
}

func (p *ParsedQuery) UpdateLimit(sourceIndex int, nSources int) {
	sLimit := sqlparser.String(p.Select.Limit.Rowcount)
	iLimit, err := strconv.Atoi(sLimit)

	if err != nil {
		slog.Error("Error parsing limit: %s", err)
	}

	baseRowCount := int(math.Floor(float64(iLimit) / float64(nSources)))
	remainder := iLimit % nSources

	if sourceIndex == 0 {
		p.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount+remainder)))
	} else if baseRowCount > 0 {
		p.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount)))
	} else {
		p.Select = nil
	}
}
