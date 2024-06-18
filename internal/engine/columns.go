package engine

import (
	"fmt"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func (q *Query) ParseColumns() error {
	for idx := range q.Main.Select.SelectExprs {
		switch expr := q.Main.Select.SelectExprs[idx].(type) {
		case *sqlparser.AliasedExpr:
			switch expr.Expr.(type) {
			case *sqlparser.ColName:
				col := Column{
					Table:     expr.Expr.(*sqlparser.ColName).Qualifier.Name.String(),
					IsGroupBy: false,
					Position:  idx,
				}
				colName := expr.Expr.(*sqlparser.ColName).Name.String()
				q.Columns[colName] = col
			case *sqlparser.FuncExpr:
				q.ParseFunctionColumns(expr, idx)
			}
		}
	}

	if q.Main.Select.GroupBy != nil {
		q.ParseGroupByColumns()
	}

	fmt.Println(q.Columns)

	return nil
}

func (q *Query) ParseJoinColumns() (string, string) {
	leftColumns := q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String()
	rightColumns := q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String()

	leftTableAlias := q.JoinDetails.JoinExpr.LeftExpr.(*sqlparser.AliasedTableExpr).As.String()
	rightTableAlias := q.JoinDetails.JoinExpr.RightExpr.(*sqlparser.AliasedTableExpr).As.String()

	for _, column := range q.Main.Select.SelectExprs {
		colName := column.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.String()
		tableName := column.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Qualifier.Name.String()

		// If the column is part of the join condition, skip it
		if (tableName == q.JoinDetails.LeftTableName || tableName == leftTableAlias) &&
			(colName == q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String() ||
				colName == q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String()) {
			continue
			// If the column is NOT part of the join condition, add it to the query, but mark it as not to be returned
		} else if tableName == q.JoinDetails.LeftTableName || tableName == leftTableAlias {
			leftColumns += "," + colName
			q.JoinDetails.ReturnJoinCols = false
		}

		// Same things as above, but for the right table
		if (tableName == q.JoinDetails.RightTableName || tableName == rightTableAlias) &&
			(colName == q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String() ||
				colName == q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String()) {
			continue
		} else if tableName == q.JoinDetails.RightTableName || tableName == rightTableAlias {
			rightColumns += "," + colName
		}
	}
	return leftColumns, rightColumns
}

func (q *Query) ParseFunctionColumns(expr *sqlparser.AliasedExpr, idx int) error {
	q.ReducerRequired = true
	q.IsAggregate = true
	col := Column{
		Table:     "",
		FuncName:  expr.Expr.(*sqlparser.FuncExpr).Name.String(),
		IsGroupBy: false,
		Position:  idx,
	}
	if expr.As.String() != "" {
		colName := expr.As.String()
		q.Columns[colName] = col
	} else {
		colName := expr.Expr.(*sqlparser.FuncExpr).Name.String()
		q.Columns[colName] = col
	}

	return nil
}

func (q *Query) ParseGroupByColumns() error {
	groupby := q.Main.Select.GroupBy

	for _, expr := range groupby {
		switch group := expr.(type) {
		case *sqlparser.ColName:
			colName := group.Name.String()
			if entry, ok := q.Columns[colName]; ok {
				fmt.Println("here1")
				entry.IsGroupBy = true
				q.Columns[colName] = entry
			}
		case *sqlparser.SQLVal:
			colVal, _ := strconv.Atoi(string(group.Val))
			for name, column := range q.Columns {
				if (colVal - 1) == column.Position {
					if entry, ok := q.Columns[name]; ok {
						fmt.Println("here")
						entry.IsGroupBy = true
						q.Columns[name] = entry
					}
				}
			}
		}
	}

	return nil
}
