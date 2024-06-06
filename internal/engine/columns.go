package engine

import (
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
				}
				colName := expr.Expr.(*sqlparser.ColName).Name.String()
				q.Columns[colName] = col
			case *sqlparser.FuncExpr:
				q.ReducerRequired = true
				q.IsAggregate = true
				col := Column{
					Table:     "",
					FuncName:  expr.Expr.(*sqlparser.FuncExpr).Name.String(),
					IsGroupBy: false,
				}
				if expr.As.String() != "" {
					colName := expr.As.String()
					q.Columns[colName] = col
				} else {
					colName := expr.Expr.(*sqlparser.FuncExpr).Name.String()
					q.Columns[colName] = col
				}
			}
		}
	}

	return nil
}

func (p *ParsedQuery) ParseJoinColumns() (string, string) {
	leftColumns := p.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String()
	rightColumns := p.JoinDetails.Condition.On.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String()

	leftTableAlias := p.JoinDetails.JoinExpr.LeftExpr.(*sqlparser.AliasedTableExpr).As.String()
	rightTableAlias := p.JoinDetails.JoinExpr.RightExpr.(*sqlparser.AliasedTableExpr).As.String()

	for _, column := range p.Select.SelectExprs {
		colName := column.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.String()
		tableName := column.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Qualifier.Name.String()
		if tableName == p.JoinDetails.LeftTableName || tableName == leftTableAlias {
			leftColumns += "," + colName
		}
		if tableName == p.JoinDetails.RightTableName || tableName == rightTableAlias {
			rightColumns += "," + colName
		}
	}
	return leftColumns, rightColumns
}
