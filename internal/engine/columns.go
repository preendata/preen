package engine

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (p *ParsedQuery) ParseColumns() error {
	for idx := range p.Select.SelectExprs {
		switch expr := p.Select.SelectExprs[idx].(type) {
		case *sqlparser.AliasedExpr:
			switch expr.Expr.(type) {
			case *sqlparser.ColName:
				table := expr.Expr.(*sqlparser.ColName).Qualifier.Name.String()
				col := Column{
					Table:    &table,
					Position: idx,
				}
				colName := expr.Expr.(*sqlparser.ColName).Name.String()
				p.OrderedColumns = append(p.OrderedColumns, colName)
				colHashKey := fmt.Sprintf("%s.%s", table, colName)
				p.Columns[colHashKey] = col
			case *sqlparser.FuncExpr:
				p.ParseFunctionColumns(expr, idx)
			}
		}
	}

	return nil
}

func (q *Query) ParseJoinColumns() (string, string) {
	leftColumns := q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String()
	rightColumns := q.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String()

	leftTableAlias := q.JoinDetails.JoinExpr.LeftExpr.(*sqlparser.AliasedTableExpr).As.String()
	rightTableAlias := q.JoinDetails.JoinExpr.RightExpr.(*sqlparser.AliasedTableExpr).As.String()

	for _, expr := range q.Main.Select.SelectExprs {
		switch expr.(type) {
		case *sqlparser.AliasedExpr:
			switch expr.(*sqlparser.AliasedExpr).Expr.(type) {
			case *sqlparser.ColName:
				colName := expr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.String()
				tableName := expr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Qualifier.Name.String()

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
		}
	}
	return leftColumns, rightColumns
}

func (p *ParsedQuery) ParseFunctionColumns(expr *sqlparser.AliasedExpr, idx int) error {
	table := "results"
	col := Column{
		Table:    &table,
		FuncName: expr.Expr.(*sqlparser.FuncExpr).Name.String(),
		Position: idx,
	}
	if expr.As.String() != "" {
		colName := expr.As.String()
		colHashKey := fmt.Sprintf("%s.%s", table, colName)
		p.OrderedColumns = append(p.OrderedColumns, colName)
		p.Columns[colHashKey] = col
	} else {
		colName := expr.Expr.(*sqlparser.FuncExpr).Name.String()
		colHashKey := fmt.Sprintf("%s.%s", table, colName)
		p.OrderedColumns = append(p.OrderedColumns, colName)
		p.Columns[colHashKey] = col
	}

	return nil
}
