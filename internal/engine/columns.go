package engine

import (
	"errors"
	"fmt"

	"github.com/hyphasql/sqlparser"
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

type TableName string
type ColumnName string
type MajorityType string
type ColumnType struct {
	// Types is a slice of every data type found for a column from its sources
	Types        []string     `json:"types"`
	MajorityType MajorityType `json:"majority_type"`
}

type ColumnMetadata map[TableName]map[ColumnName]ColumnType

// BuildColumnMetadata does 2 things:
// 1) Acts as the interface between information schema data stored in DuckDB and the parts of the application that will
// need to consume that data, in particular the model builder
// 2) Performs type validation against each column pulled from the source databases, via the Boyer-Moore majority voting
// algorithm. This majority type is then packaged into the ColumnMetadata and return to the caller. This is important
// for typing the model tables created in DuckDB
func BuildColumnMetadata() (ColumnMetadata, error) {
	// query data from hypha_information_schema
	results, err := Execute("SELECT column_name, data_type, table_name FROM hypha_information_schema")
	if err != nil {
		return nil, err
	}

	columnMetadata := buildColumnMetadataDataStructure(&results.Rows)

	// For each column in each table as sourced from InformationSchema, determine the majority type
	for tableName, tableStruct := range columnMetadata {
		for columnName, columnStruct := range tableStruct {
			majorityType, err := identifyMajorityType(columnName, columnStruct.Types)
			if err != nil {
				return nil, err
			}
			columnMetadata[tableName][columnName] = ColumnType{
				Types:        columnStruct.Types,
				MajorityType: majorityType,
			}
		}

	}

	return columnMetadata, nil
}

// Rearranges the result set from the information schema to make it easier to process for the majority type calculator
func buildColumnMetadataDataStructure(rows *[]map[string]any) ColumnMetadata {
	columnMetadata := make(ColumnMetadata)

	for _, row := range *rows {

		// Runtime panic waiting to happen. This depends on the information schema being built correctly and only with
		// type string
		tableName := TableName(row["table_name"].(string))
		columnName := ColumnName(row["column_name"].(string))
		dataType := (row["data_type"].(string))
		// Create table map if not exists
		_, exists := columnMetadata[tableName]
		if !exists {
			columnMetadata[tableName] = make(map[ColumnName]ColumnType)
		}

		// Create column map if not exists
		_, exists = columnMetadata[tableName][columnName]
		if !exists {
			columnMetadata[tableName][columnName] = ColumnType{
				Types: make([]string, 0),
			}
		}

		// Append data type to column map
		localTypesCopy := append(columnMetadata[tableName][columnName].Types, dataType)
		columnMetadata[tableName][columnName] = ColumnType{
			Types: localTypesCopy,
		}

	}

	return columnMetadata
}

// Select majority type of input column via Boyer-Moore majority vote algorithm
func identifyMajorityType(columnName ColumnName, types []string) (MajorityType, error) {
	// Implement Boyer-Moore majority vote algorithm
	var majority MajorityType
	votes := 0

	for _, candidate := range types {
		mtCandidate := MajorityType(candidate)
		if votes == 0 {
			majority = mtCandidate
		}
		if mtCandidate == majority {
			votes++
		} else {
			votes--
		}
	}

	count := 0

	// Checking if majority candidate occurs more than n/2 times
	for _, candidate := range types {
		if MajorityType(candidate) == majority {
			count += 1
		}
	}
	if majority == "" {
		Warn(
			fmt.Sprintf("Column: '%s' is missing from majority of tables!", columnName),
		)
	} else if count > len(types)/2 && count == len(types) {
		Debug(
			fmt.Sprintf("Data type for column '%s' is: %s", columnName, majority),
		)
		return majority, nil

	} else if count > len(types)/2 && count != len(types) {
		Warn(
			fmt.Sprintf("Discrepancy in data types for column '%s'! Using majority data type of %s", columnName, majority),
		)
		return majority, nil
	}

	Warn(
		fmt.Sprintf("No majority data type found for column '%s'!", columnName),
	)
	// This needs to be made unreachable
	return "unknown", fmt.Errorf("no majority data type found for column '%s'", columnName)
}

func ParseModelColumns(mc *ModelConfig, columnMetadata ColumnMetadata) error {
	cp := columnParser{
		columns:        make(map[TableName]map[ColumnName]Column),
		columnMetadata: columnMetadata,
	}
	for _, model := range mc.Models {
		switch model.Type {
		case "mongodb":
			cp.modelName = ModelName(model.Name)
			cp.tableName = TableName(model.Name)
			cp.ddlString = "hypha_source_name varchar, document json"
			cp.columns[cp.tableName] = make(map[ColumnName]Column)
			sourceColumn := Column{
				ModelName: model.Name,
				TableName: &cp.tableName,
				IsJoin:    false,
				Position:  0,
				Alias:     "hypha_source_name",
			}
			sourceColumnHashKey := ColumnName(fmt.Sprintf("%s.hypha_source_name", model.Name))
			cp.columns[cp.tableName][sourceColumnHashKey] = sourceColumn
			documentColumn := Column{
				ModelName: model.Name,
				TableName: &cp.tableName,
				IsJoin:    false,
				Position:  1,
				Alias:     "document",
			}
			documentColumnHashKey := ColumnName(fmt.Sprintf("%s.document", model.Name))
			cp.columns[cp.tableName][documentColumnHashKey] = documentColumn
		case "sql":
			cp.ddlString = "hypha_source_name varchar"
			selectStmt := model.Parsed.(*sqlparser.Select)
			for selectIdx := range selectStmt.SelectExprs {
				cp.selectIdx = selectIdx
				switch expr := selectStmt.SelectExprs[selectIdx].(type) {
				case *sqlparser.AliasedExpr:
					switch expr.Expr.(type) {
					// Process normal column.
					case *sqlparser.ColName:
						tableAlias := expr.Expr.(*sqlparser.ColName).Qualifier.Name.String()
						cp.tableName = model.TableMap[TableAlias(tableAlias)]
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
					// Process cast expression column
					case *sqlparser.ConvertExpr:
						tableAlias := expr.Expr.(*sqlparser.ConvertExpr).Expr.(*sqlparser.ColName).Qualifier.Name.String()
						cp.tableName = model.TableMap[TableAlias(tableAlias)]
						if err := processConvertColumn(expr, &cp); err != nil {
							return err
						}
					}
				case *sqlparser.StarExpr:
					return errors.New("star expressions are not supported. please specify columns explicitly")
				}
			}
		default:
			return fmt.Errorf("model type %s not supported", model.Type)
		}
		model.Columns = cp.columns
		model.DDLString = cp.ddlString
		mc.Models = append(mc.Models, model)
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
	colType := duckdbTypeMap[string(cp.columnMetadata[TableName(cp.tableName)][ColumnName(colName)].MajorityType)]
	if colType == "" {
		return fmt.Errorf("data type not found for column: %s.%s", cp.tableName, colName)
	}
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
		colType := duckdbTypeMap[string(cp.columnMetadata[tableName][ColumnName(colName)].MajorityType)]
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
		*colType = "boolean"
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

func processConvertColumn(expr *sqlparser.AliasedExpr, cp *columnParser) error {
	convertExpr := expr.Expr.(*sqlparser.ConvertExpr)
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
	colType := convertExpr.Type.Type
	cp.ddlString = fmt.Sprintf("%s, %s %s", cp.ddlString, col.Alias, colType)

	return nil
}
