package engine

import (
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/xwb1989/sqlparser"
)

// PrepareIndividualQueryForExecution coordinates all preparation of the individual nodes,
// or federated query components, for execution.
func (p *ParsedQuery) qPrepareIndividualQueryForExecution(sourceIdx int, query *Query) error {
	switch stmt := p.Statement.(type) {
	case *sqlparser.Select:
		p.Select = stmt
	}
	p.NodeParser(sourceIdx, len(config.GlobalConfig.Sources))

	// What is the point of this?
	if p.Statement != nil && query.JoinDetails.JoinExpr == nil {
		p.QueryString = make([]string, 0)
		p.QueryString = append(p.QueryString, sqlparser.String(p.Statement))
	}

	return nil
}

// ExecuteFederatedQueryComponent is the adapter between the query parsing engine and the database layer.
func (p *ParsedQuery) ExecuteFederatedQueryComponent(cfg *config.Config) error {
	p.NodeResults = make(map[string][]map[string]any)
	if p.Source.Engine == "postgres" {
		for _, query := range p.QueryString {
			if query != "no-op" {
				nodeParsed, err := sqlparser.Parse(query)

				if err != nil {
					return err
				}

				tableName := sqlparser.String(nodeParsed.(*sqlparser.Select).From[0])
				p.NodeResults[tableName] = make([]map[string]any, 0)
				result, err := pg.ExecuteRaw(query, cfg, p.Source)

				if err != nil {
					return err
				}

				p.NodeResults[tableName] = append(p.NodeResults[tableName], result.Rows...)
				p.OrderedColumns = result.Columns
			}
		}
	}
	return nil
}
