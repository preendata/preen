package engine

import (
	"errors"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/hlog"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/xwb1989/sqlparser"
)

type Join struct {
	JoinExpr       *sqlparser.JoinTableExpr
	LeftTableName  string
	RightTableName string
	Condition      *sqlparser.JoinCondition
	ReturnJoinCols bool
}

type ParsedQuery struct {
	Statement      sqlparser.Statement
	Select         *sqlparser.Select
	QueryString    []string
	Source         config.Source
	Columns        map[string]Column
	OrderedColumns []string
	Limit          *int
}

type Column struct {
	Table    *string
	FuncName string
	IsJoin   bool
	Position int
}

type QueryResults struct {
	Rows        []map[string]any
	Columns     []string
	ResultsChan chan map[string]any
}

type Query struct {
	OriginalQueryStatement string
	OrderedColumns         []string
	QueryContext           QueryContext
	Cfg                    *config.Config
	Main                   ParsedQuery
	JoinDetails            Join
	Nodes                  []ParsedQuery
	Results                QueryResults
}

// Execute executes a prepared statement on all sources in the config
func Execute(statement string, cfg *config.Config) (*QueryResults, error) {
	q := Query{
		OriginalQueryStatement: statement,
		Cfg:                    cfg,
		Nodes:                  make([]ParsedQuery, len(cfg.Sources)),
	}
	q.Main.Columns = make(map[string]Column)

	q.Results = QueryResults{
		Rows:        nil,
		Columns:     nil,
		ResultsChan: make(chan map[string]any),
	}

	parsed, err := sqlparser.Parse(q.OriginalQueryStatement)

	if err != nil {
		return nil, err
	}

	q.Main.Statement = parsed

	switch stmt := q.Main.Statement.(type) {
	case *sqlparser.Select:
		q.Main.Select = stmt
		err = q.SelectMapper()
		if err != nil {
			hlog.Debug("Error mapping select statement", q)
			return nil, err
		}
		go q.CollectResults(q.Results.ResultsChan)
		q.Main.ParseColumns()
	default:
		err = errors.New("unsupported sql statement. please provide a select statement")
		return nil, err
	}
	q.Results.Columns = q.Main.OrderedColumns
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
				hlog.Error("Error building context: ", err)
			}
		}

		err = q.Nodes[idx].ExecuteNodeQuery(q.Cfg)

		if err != nil {
			hlog.Error("Error executing node query: ", err)
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
