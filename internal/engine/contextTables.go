package engine

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func GetContextTableAliases(stmt *sqlparser.Select) map[string]string {
	tableMap := make(map[string]string)
	table := stmt.From[0]
	switch t := table.(type) {
	case *sqlparser.AliasedTableExpr:
		tableMap[t.As.String()] = t.Expr.(sqlparser.TableName).Name.String()
	case *sqlparser.JoinTableExpr:
		parseJoinTables(t, tableMap)
	default:
		fmt.Println("default")
	}

	return tableMap
}

func parseJoinTables(j *sqlparser.JoinTableExpr, tableMap map[string]string) (*sqlparser.JoinTableExpr, error) {
	rightAlias := j.RightExpr.(*sqlparser.AliasedTableExpr).As.String()
	rightTable := j.RightExpr.(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
	if rightAlias != "" {
		tableMap[rightAlias] = rightTable
	} else {
		tableMap[rightTable] = rightTable
	}

	switch left := j.LeftExpr.(type) {
	case *sqlparser.JoinTableExpr:
		parseJoinTables(left, tableMap)
	case *sqlparser.AliasedTableExpr:
		leftAlias := left.As.String()
		leftTable := left.Expr.(sqlparser.TableName).Name.String()
		if leftAlias != "" {
			tableMap[leftAlias] = leftTable
		} else {
			tableMap[leftTable] = leftTable
		}
	}

	return j, nil
}
