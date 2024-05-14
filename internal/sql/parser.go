package sql

import (
	"errors"

	"github.com/hyphadb/hyphadb/internal/config"

	"github.com/xwb1989/sqlparser"
)

type Parser struct {
	Statement sqlparser.Statement
	Cfg       *config.Config
	SourceIdx int
	Select    *sqlparser.Select
}

// Parse a SQL query and returns a parsed representation of it.
func Parse(query string, cfg *config.Config, idx int) (*string, error) {
	parsedStatement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	p := Parser{
		Statement: parsedStatement,
		Cfg:       cfg,
		SourceIdx: idx,
	}

	switch statement := p.Statement.(type) {
	case *sqlparser.Select:
		p.Select = statement
		p.SelectParser()
		if p.Select == nil {
			return nil, nil
		}
		returnString := sqlparser.String(p.Select)
		return &returnString, nil
	}

	err = errors.New("unsupported sql statement type")
	return nil, err
}
