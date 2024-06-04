package engine

import "github.com/xwb1989/sqlparser"

func (q *Query) ParseColumns() error {
	for idx := range q.Main.Select.SelectExprs {
		switch expr := q.Main.Select.SelectExprs[idx].(type) {
		case *sqlparser.AliasedExpr:
			switch expr.Expr.(type) {
			case *sqlparser.ColName:
				col := Column{
					Table:       expr.Expr.(*sqlparser.ColName).Qualifier.Name.String(),
					Name:        expr.Expr.(*sqlparser.ColName).Name.String(),
					IsAggregate: false,
					IsGroupBy:   false,
				}
				q.Columns = append(q.Columns, col)
			case *sqlparser.FuncExpr:
				col := Column{
					Table:       "",
					FuncName:    expr.Expr.(*sqlparser.FuncExpr).Name.String(),
					IsAggregate: true,
					IsGroupBy:   false,
				}
				if expr.As.String() != "" {
					col.Name = expr.As.String()
				} else {
					col.Name = expr.Expr.(*sqlparser.FuncExpr).Name.String()
				}
				q.Columns = append(q.Columns, col)
			}
		}
	}

	return nil
}
