package engine

import (
	"errors"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/xwb1989/sqlparser"
)

type FuncName string

type Column struct {
	TableName *TableName
	ModelName ModelName
	FuncName  FuncName
	IsJoin    bool
	Position  int
	Alias     string
}

type columnParser struct {
	columns        map[TableName]map[ColumnName]Column
	ddlString      string
	tableName      TableName
	modelName      ModelName
	selectIdx      int
	columnMetadata ColumnMetadata
}

func ParseModelColumns(models map[ModelName]*ModelConfig, columnMetadata ColumnMetadata) error {
	cp := columnParser{
		columns:        make(map[TableName]map[ColumnName]Column),
		columnMetadata: columnMetadata,
	}
	for modelName, modelConfig := range models {
		if !modelConfig.IsSql {
			cp.modelName = ModelName(modelName)
			cp.tableName = TableName(modelName)
			cp.ddlString = "hypha_source_name varchar, document json"
			cp.columns[cp.tableName] = make(map[ColumnName]Column)
			sourceColumn := Column{
				ModelName: modelName,
				TableName: &cp.tableName,
				IsJoin:    false,
				Position:  0,
				Alias:     "hypha_source_name",
			}
			sourceColumnHashKey := ColumnName(fmt.Sprintf("%s.hypha_source_name", modelName))
			cp.columns[cp.tableName][sourceColumnHashKey] = sourceColumn
			documentColumn := Column{
				ModelName: modelName,
				TableName: &cp.tableName,
				IsJoin:    false,
				Position:  1,
				Alias:     "document",
			}
			documentColumnHashKey := ColumnName(fmt.Sprintf("%s.document", modelName))
			cp.columns[cp.tableName][documentColumnHashKey] = documentColumn
		} else {
			cp.ddlString = "hypha_source_name varchar"
			selectStmt := modelConfig.Parsed.(*sqlparser.Select)
			for selectIdx := range selectStmt.SelectExprs {
				cp.selectIdx = selectIdx
				switch expr := selectStmt.SelectExprs[selectIdx].(type) {
				case *sqlparser.AliasedExpr:
					switch expr.Expr.(type) {
					// Process normal column.
					case *sqlparser.ColName:
						tableAlias := expr.Expr.(*sqlparser.ColName).Qualifier.Name.String()
						cp.tableName = modelConfig.TableMap[TableAlias(tableAlias)]
						if err := processModelColumn(expr, &cp); err != nil {
							return err
						}
					// Process function expression column.
					case *sqlparser.FuncExpr:
						cp.tableName = "model_generated"
						if err := processFunction(expr, &cp); err != nil {
							return err
						}
					// Process case expression column
					case *sqlparser.CaseExpr:
						cp.tableName = "model_generated"
						if err := processCase(expr, &cp); err != nil {
							return err
						}
					}

				case *sqlparser.StarExpr:
					return errors.New("star expressions are not supported. please specify columns explicitly")
				}
			}
		}
		modelConfig.Columns = cp.columns
		modelConfig.DDLString = cp.ddlString
		models[modelName] = modelConfig
	}

	return nil
}

func processModelColumn(expr *sqlparser.AliasedExpr, cp *columnParser) error {
	// We require fully qualified column names, i.e. table.column, users.user_id.
	if expr.Expr.(*sqlparser.ColName).Qualifier.Name.String() == "" {
		return errors.New("column names must be fully qualified, e.g. table.column")
	}
	if _, ok := cp.columns[cp.tableName]; !ok {
		cp.columns[cp.tableName] = make(map[ColumnName]Column)
	}
	col := Column{
		TableName: &cp.tableName,
		Position:  cp.selectIdx,
	}
	if expr.As.String() != "" {
		col.Alias = expr.As.String()
	} else {
		col.Alias = expr.Expr.(*sqlparser.ColName).Name.String()
	}
	colName := expr.Expr.(*sqlparser.ColName).Name.String()
	colHashKey := fmt.Sprintf("%s.%s", cp.tableName, colName)
	cp.columns[cp.tableName][ColumnName(colHashKey)] = col

	// Check to see if the table and column exists in the columnMetadata structure
	// If it does not exist, then we return an error since we are unable to determine
	// the appropriate data type.
	if _, ok := cp.columnMetadata[TableName(cp.tableName)][ColumnName(colName)]; !ok {
		return fmt.Errorf("column not found in table: %s.%s. check that your model query is valid", cp.tableName, colName)
	}

	// Look up the data type and append it to the table creation DDL string.
	colType := duckdb.PgTypeMap[string(cp.columnMetadata[TableName(cp.tableName)][ColumnName(colName)].MajorityType)]
	cp.ddlString = fmt.Sprintf("%s, %s %s", cp.ddlString, col.Alias, colType)

	return nil
}

func processFunction(expr *sqlparser.AliasedExpr, cp *columnParser) error {
	funcExpr := expr.Expr.(*sqlparser.FuncExpr)
	if _, ok := cp.columns[cp.tableName]; !ok {
		cp.columns[cp.tableName] = make(map[ColumnName]Column)
	}
	col := Column{
		TableName: &cp.tableName,
		FuncName:  FuncName(funcExpr.Name.String()),
		Position:  cp.selectIdx,
	}
	if expr.As.String() != "" {
		col.Alias = expr.As.String()
		colHashKey := fmt.Sprintf("%s.%s", cp.tableName, col.Alias)
		cp.columns[cp.tableName][ColumnName(colHashKey)] = col
	} else {
		col.Alias = fmt.Sprintf("\"%s\"", sqlparser.String(expr))
		colHashKey := fmt.Sprintf("%s.%s", cp.tableName, col.Alias)
		cp.columns[cp.tableName][ColumnName(colHashKey)] = col
	}

	switch col.FuncName {
	// Count always returns an integer type
	case "count":
		cp.ddlString = fmt.Sprintf("%s, %s bigint", cp.ddlString, col.Alias)
	// Average always returns a double
	case "avg":
		cp.ddlString = fmt.Sprintf("%s, %s double", cp.ddlString, col.Alias)
	// Look up the data type of the column inside the function and use that data type
	default:
		selectExpr := funcExpr.Exprs[0].(*sqlparser.AliasedExpr).Expr
		colName := selectExpr.(*sqlparser.ColName).Name.String()
		tableName := TableName(selectExpr.(*sqlparser.ColName).Qualifier.Name.String())
		if _, ok := cp.columnMetadata[tableName][ColumnName(colName)]; !ok {
			return fmt.Errorf("column not found in table: %s.%s. check that your model query is valid", cp.tableName, colName)
		}
		colType := duckdb.PgTypeMap[string(cp.columnMetadata[tableName][ColumnName(colName)].MajorityType)]
		cp.ddlString = fmt.Sprintf("%s, %s %s", cp.ddlString, col.Alias, colType)
	}

	return nil
}

func processCase(expr *sqlparser.AliasedExpr, cp *columnParser) error {
	if _, ok := cp.columns[cp.tableName]; !ok {
		cp.columns[cp.tableName] = make(map[ColumnName]Column)
	}
	col := Column{
		TableName: &cp.tableName,
		Position:  cp.selectIdx,
	}

	if expr.As.String() != "" {
		col.Alias = expr.As.String()
		colHashKey := fmt.Sprintf("%s.%s", cp.tableName, col.Alias)
		cp.columns[cp.tableName][ColumnName(colHashKey)] = col
	} else {
		col.Alias = fmt.Sprintf("\"%s\"", sqlparser.String(expr))
		colHashKey := fmt.Sprintf("%s.%s", cp.tableName, col.Alias)
		cp.columns[cp.tableName][ColumnName(colHashKey)] = col
	}

	colType := new(string)
	whens := expr.Expr.(*sqlparser.CaseExpr).Whens
	// Check the first when clause to determine the data type of the column
	// If any of the when clauses have a different data type, then the SQL engine
	// will throw an error for us.
	switch expr := whens[0].Val.(type) {
	case sqlparser.BoolVal:
		fmt.Println("boolean")
	case *sqlparser.SQLVal:
		switch expr.Type {
		case sqlparser.IntVal:
			*colType = "bigint"
		case sqlparser.StrVal:
			*colType = "varchar"
		case sqlparser.FloatVal:
			*colType = "double"
		default:
			return errors.New("unsupported data type in case expression")
		}
	default:
		return errors.New("unsupported data type in case expression")
	}
	cp.ddlString = fmt.Sprintf("%s, %s %s", cp.ddlString, col.Alias, *colType)

	return nil
}
