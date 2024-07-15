package engine

import (
	"errors"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (p *ParsedQuery) ParseColumns() error {
	for idx := range p.Select.SelectExprs {
		switch expr := p.Select.SelectExprs[idx].(type) {
		case *sqlparser.AliasedExpr:
			switch expr.Expr.(type) {
			case *sqlparser.ColName:
				if expr.Expr.(*sqlparser.ColName).Qualifier.Name.String() == "" {
					return errors.New("Column names must be fully qualified, e.g. table.column")
				}
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
		case *sqlparser.StarExpr:
			return errors.New("star expressions are not supported. please specify columns explicitly")
		}
	}

	return nil
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

func (q *Query) ParseJoinColumns(j *sqlparser.JoinTableExpr) (*sqlparser.JoinTableExpr, error) {
	rightTable := j.RightExpr.(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	switch col := j.Condition.On.(type) {
	case *sqlparser.ComparisonExpr:
		leftTable := col.Left.(*sqlparser.ColName).Qualifier.Name.String()
		rightCol := col.Right.(*sqlparser.ColName).Name.String()
		leftCol := col.Left.(*sqlparser.ColName).Name.String()
		q.processColumn(rightTable, rightCol)
		q.processColumn(leftTable, leftCol)
	case *sqlparser.AndExpr:
		q.joinConditionFlatten(rightTable, col)
	}
	switch left := j.LeftExpr.(type) {
	case *sqlparser.JoinTableExpr:
		switch col := left.Condition.On.(type) {
		case *sqlparser.ComparisonExpr:
			leftTable := col.Left.(*sqlparser.ColName).Qualifier.Name.String()
			rightCol := col.Right.(*sqlparser.ColName).Name.String()
			leftCol := col.Left.(*sqlparser.ColName).Name.String()
			q.processColumn(rightTable, rightCol)
			q.processColumn(leftTable, leftCol)
		case *sqlparser.AndExpr:
			q.joinConditionFlatten(rightTable, col)
		}
		return q.ParseJoinColumns(left)
	case *sqlparser.AliasedTableExpr:
		leftTable := left.Expr.(sqlparser.TableName).Name.String()
		switch col := j.Condition.On.(type) {
		case *sqlparser.ComparisonExpr:
			rightCol := col.Right.(*sqlparser.ColName).Name.String()
			leftCol := col.Left.(*sqlparser.ColName).Name.String()
			q.processColumn(rightTable, rightCol)
			q.processColumn(leftTable, leftCol)
		case *sqlparser.AndExpr:
			q.joinConditionFlatten(rightTable, col)
		}
	}
	return nil, nil
}

func (q *Query) joinConditionFlatten(rightTable string, a *sqlparser.AndExpr) (*sqlparser.AndExpr, error) {
	switch col := a.Left.(type) {
	case *sqlparser.ComparisonExpr:
		colName := col.Right.(*sqlparser.ColName).Name.String()
		q.processColumn(rightTable, colName)
	case *sqlparser.AndExpr:
		return q.joinConditionFlatten(rightTable, col)
	}
	return nil, nil
}

func (q *Query) processColumn(tableName string, columnName string) error {
	colHashKey := fmt.Sprintf("%s.%s", tableName, columnName)
	_, ok := q.Main.Columns[colHashKey]
	if ok {
		return errors.New("column already exists")
	}

	column := Column{
		Table:    &tableName,
		Position: len(q.Main.Columns) + 1,
	}

	q.Main.Columns[colHashKey] = column

	return nil
}
