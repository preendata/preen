package engine

import (
	"errors"
	"reflect"

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
	NodeResults    map[string][]map[string]any
	OrderedColumns []string
	Limit          *int
}

// Column represents the identity data of a column as well as if it is being utilized for aggregations or joins
type Column struct {
	Table     string
	FuncName  string
	IsGroupBy bool
	IsJoin    bool
}

type Query struct {
	OriginalQueryStatement string
	Cfg                    *config.Config
	Main                   ParsedQuery
	JoinDetails            Join
	// Nodes is a list of parsed queries for each source in the config
	Nodes           []ParsedQuery
	Results         chan map[string]any
	Columns         map[string]Column
	ReducerRequired bool
	IsAggregate     bool
	Err             error
}

// IntegratedResultSet is final product of a federated query execution. It is the effective unioning of multiple query results.
type IntegratedResultSet struct {
	Rows    []map[string]any
	Columns []string
}

// Execute executes a prepared statement on all sources in the config
func Execute(statement string, cfg *config.Config) (IntegratedResultSet, error) {
	q := Query{
		OriginalQueryStatement: statement,
		Cfg:                    cfg,
		Nodes:                  make([]ParsedQuery, len(cfg.Sources)),
		Results:                make(chan map[string]any),
		Columns:                make(map[string]Column, 0),
	}

	qr := IntegratedResultSet{
		Rows: nil,
	}

	parsed, err := sqlparser.Parse(q.OriginalQueryStatement)

	if err != nil {
		return IntegratedResultSet{}, err
	}

	q.Main.Statement = parsed

	switch stmt := q.Main.Statement.(type) {
	case *sqlparser.Select:
		q.Main.Select = stmt
		err = q.SelectMapper()
		if err != nil {
			hlog.Debug("Error mapping select statement", q)
			return IntegratedResultSet{}, err
		}
		go qr.CollectResults(q.Results)
		q.ParseColumns()
		q.SelectReducer()
	default:
		err = errors.New("unsupported sql statement. please provide a select statement")
		return IntegratedResultSet{}, err
	}

	qr.Columns = q.Main.OrderedColumns

	return qr, nil
}

func (q *Query) SelectMapper() error {
	q.MainParser()
	for idx, source := range q.Cfg.Sources {
		q.Nodes[idx].Source = source
		q.Nodes[idx].Statement, _ = sqlparser.Parse(q.OriginalQueryStatement)

		err := q.Nodes[idx].qPrepareIndividualQueryForExecution(idx, q)

		if err != nil {
			return err
		}

		err = q.Nodes[idx].ExecuteFederatedQueryComponent(q.Cfg)

		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Query) SelectReducer() error {
	// TODO - connect all reducers to a parent class that will handle collection of results with common methods
	// For now, just grab OrderedColumns from the first node, which sucks
	q.Main.OrderedColumns = q.Nodes[0].OrderedColumns

	if q.ReducerRequired {
		q.Reduce()
	} else {
		for idx := range q.Nodes {
			if len(q.Nodes[idx].NodeResults) != 0 {
				keys := reflect.ValueOf(q.Nodes[idx].NodeResults).MapKeys()
				firstKey := keys[0].String()
				for _, row := range q.Nodes[idx].NodeResults[firstKey] {
					q.Results <- row
				}
			}
		}
	}
	return nil
}

// CollectResults collects results from the individual result set channel and appends them to the IntegratedResultSet
func (qr *IntegratedResultSet) CollectResults(c chan map[string]any) {
	for row := range c {
		qr.Rows = append(qr.Rows, row)
	}
}
