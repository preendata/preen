package engine

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (q *Query) JoinNodeQuery() error {
	join := q.JoinDetails.JoinExpr
	q.JoinDetails.LeftTableName = sqlparser.String(join.LeftExpr)
	q.JoinDetails.RightTableName = sqlparser.String(join.RightExpr)
	q.JoinDetails.Condition = &join.Condition

	leftColumns, rightColumns := q.ParseJoinColumns()

	leftTableQuery := fmt.Sprintf("select %v from %v", leftColumns, q.JoinDetails.LeftTableName)
	rightTableQuery := fmt.Sprintf("select %v from %v", rightColumns, q.JoinDetails.RightTableName)

	for idx := range q.Nodes {
		q.Nodes[idx].QueryString = append(q.Nodes[idx].QueryString, leftTableQuery, rightTableQuery)
	}

	return nil
}
