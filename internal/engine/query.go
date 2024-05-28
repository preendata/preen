package engine

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/xwb1989/sqlparser"
)

type Join struct {
	JoinExpr       *sqlparser.JoinTableExpr
	LeftTableName  string
	RightTableName string
	Condition      *sqlparser.JoinCondition
}

type ParsedQuery struct {
	Statement   sqlparser.Statement
	Select      *sqlparser.Select
	JoinDetails Join
	QueryString []string
	Source      config.Source
	NodeResults map[string][]map[string]any
}

type Query struct {
	Input           string
	Cfg             *config.Config
	Main            ParsedQuery
	Nodes           []ParsedQuery
	Results         []map[string]any
	ReducerRequired bool
	Err             error
}

// Execute executes a prepared statement on all sources in the config
func Execute(statement string, cfg *config.Config) ([]map[string]any, error) {
	q := Query{
		Input:   statement,
		Cfg:     cfg,
		Nodes:   make([]ParsedQuery, len(cfg.Sources)),
		Results: make([]map[string]any, 0),
	}

	parsed, err := sqlparser.Parse(q.Input)

	if err != nil {
		return nil, err
	}

	q.Main.Statement = parsed

	switch stmt := q.Main.Statement.(type) {
	case *sqlparser.Select:
		q.Main.Select = stmt
		err = q.SelectMapper()
		if err != nil {
			return nil, err
		}
		q.SelectReducer()
	default:
		err = errors.New("unsupported sql statement. please provide a select statement")
		return nil, err
	}
	fmt.Println(len(q.Results))
	return q.Results[0:1], nil
}

func (q *Query) SelectMapper() error {
	for idx, source := range q.Cfg.Sources {
		q.Nodes[idx].Source = source
		q.Nodes[idx].QueryString = make([]string, 0)
		q.Nodes[idx].Statement, _ = sqlparser.Parse(q.Input)
		switch stmt := q.Nodes[idx].Statement.(type) {
		case *sqlparser.Select:
			q.Nodes[idx].Select = stmt
		}
		q.Nodes[idx].SelectParser(idx, len(q.Cfg.Sources))

		if q.Nodes[idx].JoinDetails.JoinExpr != nil {
			q.ReducerRequired = true
			err := q.Nodes[idx].JoinNodeQuery()
			if err != nil {
				return err
			}
		} else {
			q.Nodes[idx].QueryString = append(q.Nodes[idx].QueryString, sqlparser.String(q.Nodes[idx].Statement))
		}

		err := q.Nodes[idx].Map(q.Cfg)

		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Query) SelectReducer() (*Query, error) {
	if q.ReducerRequired {
		q.Reduce()
	} else {
		for idx := range q.Nodes {
			keys := reflect.ValueOf(q.Nodes[idx].NodeResults).MapKeys()
			firstKey := keys[0].String()
			q.Results = append(q.Results, q.Nodes[idx].NodeResults[firstKey]...)
		}
	}

	return q, nil
}
