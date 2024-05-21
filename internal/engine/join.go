package engine

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (q *ParsedQuery) Join() []map[string]any {
	join := q.JoinExpr
	leftTableName := sqlparser.String(join.LeftExpr)
	rightTableName := sqlparser.String(join.RightExpr)
	joinType := join.Join
	joinCondition := sqlparser.String(join.Condition)

	leftRows := collectRows(leftTableName)
	rightRows := collectRows(rightTableName)

	fmt.Println(joinType, joinCondition, leftRows, rightRows)
	return nil
}

func collectRows(tableName string) []map[string]any {
	fmt.Println(tableName)
	return nil
}
