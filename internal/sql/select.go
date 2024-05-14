package sql

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func (p *Parser) SelectParser() *Parser {

	if p.Select.Limit.Rowcount != nil {
		p.UpdateLimit()
	}

	return p
}

func (p *Parser) UpdateLimit() {
	nSources := len(p.Cfg.Sources)
	sLimit := sqlparser.String(p.Select.Limit.Rowcount)
	iLimit, err := strconv.Atoi(sLimit)

	if err != nil {
		slog.Error("Error parsing limit: %s", err)
	}

	baseRowCount := int(math.Floor(float64(iLimit) / float64(nSources)))
	remainder := iLimit % nSources

	if p.SourceIdx == 0 {
		p.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount+remainder)))
	} else if baseRowCount > 0 {
		p.Select.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", baseRowCount)))
	} else {
		p.Select = nil
	}
}
