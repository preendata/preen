package engine

import (
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/xwb1989/sqlparser"
)

var mutex = &sync.Mutex{}

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

func (q *Query) JoinReducer() (*Query, error) {
	var wg sync.WaitGroup
	leftColumn := q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name
	joinHash := q.JoinHashMap()
	allRows := false

	joinStr := strings.ToLower(q.JoinDetails.JoinExpr.Join)
	if strings.Contains(joinStr, "left") {
		allRows = true
	}

	for nodeIndex := range q.Nodes {
		leftRows := q.Nodes[nodeIndex].NodeResults[q.JoinDetails.LeftTableName]
		for _, leftRow := range leftRows {
			wg.Add(1)
			go func(leftRow map[string]any) {
				defer wg.Done()
				defer mutex.Unlock()

				mutex.Lock()
				q.lookup(leftColumn.String(), leftRow, joinHash, allRows)
			}(leftRow)
		}
	}
	wg.Wait()

	return q, nil
}

func (q *Query) JoinHashMap() map[any][]map[string]any {
	joinHash := make(map[any][]map[string]any)
	rightColumn := q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name

	for nodeIndex := range q.Nodes {
		rightRows := q.Nodes[nodeIndex].NodeResults[q.JoinDetails.RightTableName]
		for _, rightRow := range rightRows {
			hashKey := rightRow[rightColumn.String()]
			joinHash[hashKey] = append(joinHash[hashKey], rightRow)
		}
	}
	return joinHash
}

func (q *Query) lookup(leftColumn string, row map[string]any, joinHash map[any][]map[string]any, allRows bool) {

	hashLookup := row[leftColumn]
	if rightRows, ok := joinHash[hashLookup]; ok {
		for _, rightRow := range rightRows {
			if !q.JoinDetails.ReturnJoinCols {
				delete(rightRow, q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String())
				delete(row, q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String())
			}
			maps.Copy(row, rightRow)
			q.Results <- row
		}
	} else if !ok && allRows {
		for _, rightRow := range rightRows {
			if !q.JoinDetails.ReturnJoinCols {
				delete(rightRow, q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String())
				delete(row, q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String())
			}
			maps.Copy(row, rightRow)
			q.Results <- row
		}
	}
}
