package engine

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

// Parse a SQL query and returns a parsed representation of it.
func Parse(q ParsedQuery, idx int, nSources int) (*ParsedQuery, error) {
	parsedStatement, err := sqlparser.Parse(q.QueryString)
	if err != nil {
		return nil, err
	}

	switch statement := parsedStatement.(type) {
	case *sqlparser.Select:
		q.Select = statement
		q.SelectParser(idx, nSources)
		if q.Select == nil {
			return nil, nil
		}
		q.QueryString = sqlparser.String(q.Select)
		return &q, nil
	}

	err = errors.New("unsupported sql statement type")
	return nil, err
}
