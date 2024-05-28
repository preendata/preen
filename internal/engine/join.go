package engine

import (
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"

	"github.com/xwb1989/sqlparser"
)

var mutex = &sync.Mutex{}

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
	joinHash := q.JoinHashMap()
	allRows := false
	if q.Main.Select.Limit != nil {
		sLimit := sqlparser.String(q.Main.Select.Limit.Rowcount)
		_, err := strconv.Atoi(sLimit)

		if err != nil {
			slog.Error("Error parsing limit: %s", err)
		}
	}

	joinStr := strings.ToLower(q.Nodes[0].JoinDetails.JoinExpr.Join)
	if strings.Contains(joinStr, "left") {
		allRows = true
	}

	for nodeIndex := range q.Nodes {
		leftRows := q.Nodes[nodeIndex].NodeResults[q.Nodes[nodeIndex].JoinDetails.LeftTableName]

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
	rightColumn := q.Nodes[0].JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name

	for nodeIndex := range q.Nodes {
		rightRows := q.Nodes[nodeIndex].NodeResults[q.Nodes[nodeIndex].JoinDetails.RightTableName]
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
			maps.Copy(row, rightRow)
			q.Results = append(q.Results, row)
		}
	} else if !ok && allRows {
		for _, rightRow := range rightRows {
			maps.Copy(row, rightRow)
			q.Results = append(q.Results, row)
		}
	}
}