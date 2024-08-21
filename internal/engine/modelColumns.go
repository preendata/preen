package engine

import (
	"errors"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/xwb1989/sqlparser"
)

type Column struct {
	Table    *string
	FuncName string
	IsJoin   bool
	Position int
	Alias    string
}

type columnParser struct {
	columns        map[string]map[string]Column
	ddlString      string
	table          string
	selectIdx      int
	columnMetadata ColumnMetadata
}

func ParseModelColumns(modelQueries map[string]ModelQuery, columnMetadata ColumnMetadata) (map[string]ModelQuery, error) {
	cp := columnParser{
		columns:        make(map[string]map[string]Column),
		columnMetadata: columnMetadata,
	}
	for modelName, modelQuery := range modelQueries {
		if !modelQuery.IsSql {
			cp.table = modelName
			cp.ddlString = "hypha_source_name varchar, document json"
			cp.columns[modelName] = make(map[string]Column)
			sourceColumn := Column{
				Table:    &modelName,
				IsJoin:   false,
				Position: 0,
				Alias:    "hypha_source_name",
			}
			sourceColumnHashKey := fmt.Sprintf("%s.hypha_source_name", modelName)
			cp.columns[modelName][sourceColumnHashKey] = sourceColumn
			documentColumn := Column{
				Table:    &modelName,
				IsJoin:   false,
				Position: 1,
				Alias:    "document",
			}
			documentColumnHashKey := fmt.Sprintf("%s.document", modelName)
			cp.columns[modelName][documentColumnHashKey] = documentColumn
		} else {
			cp.ddlString = "hypha_source_name varchar"
			selectStmt := modelQuery.Parsed.(*sqlparser.Select)
			tableMap := GetModelTableAliases(selectStmt)
			for selectIdx := range selectStmt.SelectExprs {
				cp.selectIdx = selectIdx
				switch expr := selectStmt.SelectExprs[selectIdx].(type) {
				case *sqlparser.AliasedExpr:
					switch expr.Expr.(type) {
					case *sqlparser.ColName:
						tableAlias := expr.Expr.(*sqlparser.ColName).Qualifier.Name.String()
						cp.table = tableMap[tableAlias]
						cpUpdated, err := processModelColumn(expr, &cp)
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
		}
		modelQuery.Columns = cp.columns
		modelQuery.DDLString = cp.ddlString
		modelQueries[modelName] = modelQuery
	}

	return modelQueries, nil
}

func processModelColumn(expr *sqlparser.AliasedExpr, cp *columnParser) (*columnParser, error) {
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
	if expr.As.String() != "" {
		col.Alias = expr.As.String()
	} else {
		col.Alias = expr.Expr.(*sqlparser.ColName).Name.String()
	}
	colName := expr.Expr.(*sqlparser.ColName).Name.String()
	colHashKey := fmt.Sprintf("%s.%s", cp.table, colName)
	cp.columns[cp.table][colHashKey] = col
	if _, ok := cp.columnMetadata[TableName(cp.table)][ColumnName(colName)]; !ok {
		return nil, fmt.Errorf("column not found in table: %s.%s. check that your model query is valid", cp.table, colName)
	}
	colType := duckdb.PgTypeMap[string(cp.columnMetadata[TableName(cp.table)][ColumnName(colName)].MajorityType)]
	cp.ddlString = fmt.Sprintf("%s, %s %s", cp.ddlString, col.Alias, colType)

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
