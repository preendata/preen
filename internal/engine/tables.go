package engine

import (
	"fmt"
	"slices"

	"github.com/preendata/sqlparser"
)

type TableAlias string
type TableMap map[TableAlias]TableName
type TableSet []TableName

func ParseModelTables(mc *ModelConfig) error {
	for _, model := range mc.Models {
		if model.Type == "database" && model.Parsed != nil {
			switch stmt := model.Parsed.(type) {
			case *sqlparser.Select:
				model.TableMap, model.TableSet = getModelTableAliases(stmt)
			default:
				return fmt.Errorf("model %s failed. non-select queries not supported", model.Name)
			}
		}
	}
	return nil
}

func getModelTableAliases(stmt *sqlparser.Select) (TableMap, TableSet) {
	tableMap := make(TableMap)
	tableSet := make(TableSet, 0)
	table := stmt.From[0]
	switch t := table.(type) {
	case *sqlparser.AliasedTableExpr:
		if t.As.IsEmpty() {
			tableName := TableName(t.Expr.(sqlparser.TableName).Name.String())
			tableMap[TableAlias(t.Expr.(sqlparser.TableName).Name.String())] = tableName
			if !slices.Contains(tableSet, tableName) {
				tableSet = append(tableSet, tableName)
			}
		} else {
			tableName := TableName(t.Expr.(sqlparser.TableName).Name.String())
			tableMap[TableAlias(t.As.String())] = tableName
			if !slices.Contains(tableSet, tableName) {
				tableSet = append(tableSet, tableName)
			}
		}
	case *sqlparser.JoinTableExpr:
		_, joinTables := parseJoinTables(t, tableMap, tableSet)
		tableSet = append(tableSet, joinTables...)
	}

	return tableMap, tableSet
}

func parseJoinTables(j *sqlparser.JoinTableExpr, tableMap TableMap, tableSet TableSet) (*sqlparser.JoinTableExpr, TableSet) {
	rightAlias := j.RightExpr.(*sqlparser.AliasedTableExpr).As.String()
	rightTable := j.RightExpr.(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
	if rightAlias != "" {
		tableMap[TableAlias(rightAlias)] = TableName(rightTable)
		if !slices.Contains(tableSet, TableName(rightTable)) {
			tableSet = append(tableSet, TableName(rightTable))
		}
	} else {
		tableMap[TableAlias(rightTable)] = TableName(rightTable)
		if !slices.Contains(tableSet, TableName(rightTable)) {
			tableSet = append(tableSet, TableName(rightTable))
		}
	}

	switch left := j.LeftExpr.(type) {
	case *sqlparser.JoinTableExpr:
		_, tableSet = parseJoinTables(left, tableMap, tableSet)
	case *sqlparser.AliasedTableExpr:
		leftAlias := left.As.String()
		leftTable := left.Expr.(sqlparser.TableName).Name.String()
		if leftAlias != "" {
			tableMap[TableAlias(leftAlias)] = TableName(leftTable)
			if !slices.Contains(tableSet, TableName(leftTable)) {
				tableSet = append(tableSet, TableName(leftTable))
			}
		} else {
			tableMap[TableAlias(leftTable)] = TableName(leftTable)
			if !slices.Contains(tableSet, TableName(leftTable)) {
				tableSet = append(tableSet, TableName(leftTable))
			}
		}
	}
	return j, tableSet
}
