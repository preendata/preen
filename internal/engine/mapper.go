package engine

import (
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/xwb1989/sqlparser"
)

func (p *ParsedQuery) PrepareNodeQuery(sourceIdx int, query *Query) error {
	switch stmt := p.Statement.(type) {
	case *sqlparser.Select:
		p.Select = stmt
	}
	p.NodeParser(sourceIdx, len(config.GlobalConfig.Sources))

	if p.Statement != nil && query.JoinDetails.JoinExpr == nil {
		p.QueryString = make([]string, 0)
		p.QueryString = append(p.QueryString, sqlparser.String(p.Statement))
	}

	return nil
}

// ExecuteNodeQuery is the adapter between the query parsing engine and the database layer.
func (p *ParsedQuery) ExecuteNodeQuery(cfg *config.Config) error {
	if p.Source.Engine == "postgres" {
		for _, query := range p.QueryString {
			if query != "no-op" {
				nodeParsed, err := sqlparser.Parse(query)

				if err != nil {
					return err
				}

				tableName := sqlparser.String(nodeParsed.(*sqlparser.Select).From[0])
				result, err := pg.ExecuteRaw(query, cfg, p.Source)

				if err != nil {
					return err
				}

				err = p.InsertResults(tableName, result.Rows)

				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
