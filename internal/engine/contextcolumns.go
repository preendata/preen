package engine

import (
	"errors"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/xwb1989/sqlparser"
)

type Column struct {
	Table    *string
	FuncName string
	IsJoin   bool
	Position int
}

type columnParser struct {
	columns   map[string]map[string]Column
	ddlString string
	table     string
	selectIdx int
	validator pg.Validator
}

func ParseContextColumns(contextQueries map[string]ContextQuery, v pg.Validator) (map[string]ContextQuery, error) {
	cp := columnParser{
		columns:   make(map[string]map[string]Column),
		validator: v,
	}
	for contextName, contextQuery := range contextQueries {
		cp.ddlString = "hypha_source_name varchar"
		selectStmt := contextQuery.Parsed.(*sqlparser.Select)
		cp.table = selectStmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
		for selectIdx := range selectStmt.SelectExprs {
			cp.selectIdx = selectIdx
			switch expr := selectStmt.SelectExprs[selectIdx].(type) {
			case *sqlparser.AliasedExpr:
				switch expr.Expr.(type) {
				case *sqlparser.ColName:
					cpUpdated, err := processContextColumn(expr, &cp)
					if err != nil {
						return nil, err
					}
					cp = *cpUpdated
				case *sqlparser.FuncExpr:
					cpUpdated, err := processFunction(expr, &cp)
					if err != nil {
						return nil, err
					}
					cp = *cpUpdated
				}
			case *sqlparser.StarExpr:
				return nil, errors.New("star expressions are not supported. please specify columns explicitly")
			}
		}
		contextQuery.Columns = cp.columns
		contextQuery.DDLString = cp.ddlString
		contextQueries[contextName] = contextQuery
	}

	return contextQueries, nil
}

func processContextColumn(expr *sqlparser.AliasedExpr, cp *columnParser) (*columnParser, error) {
	if expr.Expr.(*sqlparser.ColName).Qualifier.Name.String() == "" {
		return nil, errors.New("column names must be fully qualified, e.g. table.column")
	}
	if _, ok := cp.columns[cp.table]; !ok {
		cp.columns[cp.table] = make(map[string]Column)
	}
	col := Column{
		Table:    &cp.table,
		Position: cp.selectIdx,
	}
	colName := expr.Expr.(*sqlparser.ColName).Name.String()
	colHashKey := fmt.Sprintf("%s.%s", cp.table, colName)
	cp.columns[cp.table][colHashKey] = col
	if _, ok := cp.validator.ColumnTypes[cp.table][colName]; !ok {
		return nil, fmt.Errorf("column not found in table: %s.%s. check that your context query is valid", cp.table, colName)
	}
	colType := duckdb.PgTypeMap[cp.validator.ColumnTypes[cp.table][colName].MajorityType]
	cp.ddlString = fmt.Sprintf("%s, %s %s", cp.ddlString, colName, colType)

	return cp, nil
}

func processFunction(expr *sqlparser.AliasedExpr, cp *columnParser) (*columnParser, error) {
	if _, ok := cp.columns[cp.table]; !ok {
		cp.columns[cp.table] = make(map[string]Column)
	}
	col := Column{
		Table:    &cp.table,
		FuncName: expr.Expr.(*sqlparser.FuncExpr).Name.String(),
		Position: cp.selectIdx,
	}
	if expr.As.String() != "" {
		colName := expr.As.String()
		colHashKey := fmt.Sprintf("%s.%s", cp.table, colName)
		cp.columns[cp.table][colHashKey] = col
	} else {
		colName := expr.Expr.(*sqlparser.FuncExpr).Name.String()
		colHashKey := fmt.Sprintf("%s.%s", cp.table, colName)
		cp.columns[cp.table][colHashKey] = col
	}

	return cp, nil
}
