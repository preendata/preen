package engine

import (
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/xwb1989/sqlparser"
)

func (p *ParsedQuery) Map(cfg *config.Config) error {
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

// func columnValidation(orderedCols *[]string, currentResultCols []string) error {
// 	if len(*orderedCols) != len(currentResultCols) {
// 		return fmt.Errorf("columns do not match")
// 	}

// 	for i, col := range *orderedCols {
// 		if col != currentResultCols[i] {
// 			return fmt.Errorf("columns do not match")
// 		}
// 	}

// 	return nil
// }
