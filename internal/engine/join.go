package engine

import (
	"maps"
	"reflect"
	"sync"

	"github.com/xwb1989/sqlparser"
)

func (p *ParsedQuery) JoinNodeQuery() error {
	join := p.JoinDetails.JoinExpr
	p.JoinDetails.LeftTableName = sqlparser.String(join.LeftExpr)
	p.JoinDetails.RightTableName = sqlparser.String(join.RightExpr)
	p.JoinDetails.Condition = &join.Condition

	leftTableQuery := "select * from " + p.JoinDetails.LeftTableName
	rightTableQuery := "select * from " + p.JoinDetails.RightTableName

	p.QueryString = append(p.QueryString, leftTableQuery, rightTableQuery)

	return nil
}

func (q *Query) JoinReducer() (*Query, error) {
	var wg sync.WaitGroup
	// operator := q.Nodes[0].JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Operator
	leftColumn := q.Nodes[0].JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name
	rightColumn := q.Nodes[0].JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name

	for idx := range q.Nodes {
		wg.Add(1)
		leftRows := q.Nodes[idx].NodeResults[q.Nodes[idx].JoinDetails.LeftTableName]

		go func(idx int) {
			for _, leftRow := range leftRows {
				rightRows := q.Nodes[idx].NodeResults[q.Nodes[idx].JoinDetails.RightTableName]
				for _, rightRow := range rightRows {
					if reflect.DeepEqual(leftRow[leftColumn.String()], rightRow[rightColumn.String()]) {
						maps.Copy(leftRow, rightRow)
						q.Results = append(q.Results, leftRow)
					}
				}
			}
			defer wg.Done()
		}(idx)
	}

	wg.Wait()

	return q, nil
}
