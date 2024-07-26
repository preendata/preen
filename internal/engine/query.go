package engine

import (
	"errors"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/utils"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/xwb1989/sqlparser"
)

type Join struct {
	JoinExpr       *sqlparser.JoinTableExpr
	LeftTableName  string
	RightTableName string
	Condition      *sqlparser.JoinCondition
}

type QueryResults struct {
	Rows        []map[string]any
	Columns     []string
	ResultsChan chan map[string]any
}

type Query struct {
	OriginalQueryStatement string
	OrderedColumns         []string
	Columns                map[string]Column
	Statement              sqlparser.Statement
	Select                 *sqlparser.Select
	QueryContext           QueryContext
	Cfg                    *config.Config
	JoinDetails            Join
	Results                QueryResults
}

// Execute executes a prepared statement on all sources in the config
func Execute(statement string, cfg *config.Config) (*QueryResults, error) {
	utils.Info("Executing query...")
	q := Query{
		OriginalQueryStatement: statement,
		Cfg:                    cfg,
	}
	q.Columns = make(map[string]Column)

	q.Results = QueryResults{
		Rows:        nil,
		Columns:     nil,
		ResultsChan: make(chan map[string]any),
	}

	parsed, err := sqlparser.Parse(q.OriginalQueryStatement)

	if err != nil {
		return nil, err
	}

	q.Statement = parsed

	switch stmt := q.Statement.(type) {
	case *sqlparser.Select:
		q.Select = stmt

		q.OrderedColumns, q.Columns, err = ParseColumns(q.Select)
		if err != nil {
			utils.Debug("Error parsing columns", q)
			return nil, err
		}

		err = q.SelectMapper()
		if err != nil {
			utils.Debug("Error mapping select statement", q)
			return nil, err
		}
		go q.CollectResults(q.Results.ResultsChan)
	default:
		err = errors.New("unsupported sql statement. please provide a select statement")
		return nil, err
	}
	q.Results.Columns = q.OrderedColumns
	err = duckdb.Query(q.OriginalQueryStatement, q.Results.ResultsChan)

	if err != nil {
		return nil, err
	}

	return &q.Results, nil
}

func (q *Query) SelectMapper() error {
	q.MainParser()
	for idx, source := range q.Cfg.Sources {
		q.Nodes[idx].Source = source
		q.Nodes[idx].Columns = make(map[string]Column)
		q.Nodes[idx].Statement, _ = sqlparser.Parse(q.OriginalQueryStatement)

		err := q.Nodes[idx].PrepareNodeQuery(idx, q)

		if err != nil {
			return err
		}

		if !q.QueryContext.Valid {
			err := q.BuildContext()

			if err != nil {
				utils.Error("Error building context: ", err)
			}
		}

		err = q.Nodes[idx].ExecuteNodeQuery(q.Cfg)

		if err != nil {
			utils.Error("Error executing node query: ", err)
		}
	}
	return nil
}

func (q *Query) CollectResults(c chan map[string]any) error {
	for row := range c {
		q.Results.Rows = append(q.Results.Rows, row)
	}
	return nil
}
