package engine

import (
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/xwb1989/sqlparser"
)

type ParsedQuery struct {
	Statement   sqlparser.Statement
	Select      *sqlparser.Select
	Join        *sqlparser.JoinTableExpr
	QueryString string
}

type Query struct {
	Input string
	Cfg   *config.Config
	Main  ParsedQuery
	Node  ParsedQuery
}

// Execute executes a prepared statement on all sources in the config
func Execute(statement string, cfg *config.Config) ([]map[string]any, error) {
	q := Query{
		Input: statement,
		Cfg:   cfg,
	}
	results := []map[string]any{}

	parsed, err := sqlparser.Parse(statement)

	if err != nil {
		return nil, err
	}

	q.Main.Statement = parsed

	for idx, source := range cfg.Sources {
		q.Node.Statement, _ = sqlparser.Parse(statement)
		switch stmt := q.Node.Statement.(type) {
		case *sqlparser.Select:
			q.Node.Select = stmt
		}
		q.Node.SelectParser(idx, len(cfg.Sources))
		q.Node.QueryString = sqlparser.String(q.Node.Statement)

		if source.Engine == "postgres" {
			result, err := pg.ExecuteRaw(q.Node.QueryString, q.Cfg, source)
			if err != nil {
				return nil, err
			}
			results = append(results, result...)
		}
	}
	return results, nil
}
