package engine

import (
	"errors"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

type Column struct {
	Table    *string
	FuncName string
	IsJoin   bool
	Position int
}

func ParseColumns(s *sqlparser.Select) ([]string, map[string]Column, error) {
	columns := make(map[string]Column)
	orderedColumns := make([]string, 0)

	for idx := range s.SelectExprs {
		switch expr := s.SelectExprs[idx].(type) {
		case *sqlparser.AliasedExpr:
			switch expr.Expr.(type) {
			case *sqlparser.ColName:
				if expr.Expr.(*sqlparser.ColName).Qualifier.Name.String() == "" {
					return nil, nil, errors.New("Column names must be fully qualified, e.g. table.column")
				}
				table := expr.Expr.(*sqlparser.ColName).Qualifier.Name.String()
				col := Column{
					Table:    &table,
					Position: idx,
				}
				colName := expr.Expr.(*sqlparser.ColName).Name.String()
				orderedColumns = append(orderedColumns, colName)
				colHashKey := fmt.Sprintf("%s.%s", table, colName)
				columns[colHashKey] = col
			case *sqlparser.FuncExpr:
				table := "results"
				col := Column{
					Table:    &table,
					FuncName: expr.Expr.(*sqlparser.FuncExpr).Name.String(),
					Position: idx,
				}
				if expr.As.String() != "" {
					colName := expr.As.String()
					colHashKey := fmt.Sprintf("%s.%s", table, colName)
					orderedColumns = append(orderedColumns, colName)
					columns[colHashKey] = col
				} else {
					colName := expr.Expr.(*sqlparser.FuncExpr).Name.String()
					colHashKey := fmt.Sprintf("%s.%s", table, colName)
					orderedColumns = append(orderedColumns, colName)
					columns[colHashKey] = col
				}

			}
		case *sqlparser.StarExpr:
			return nil, nil, errors.New("star expressions are not supported. please specify columns explicitly")
		}
	}

	return orderedColumns, columns, nil
}

func ParseJoinColumns(j *sqlparser.JoinTableExpr) (*sqlparser.JoinTableExpr, error) {
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

func joinConditionFlatten(rightTable string, a *sqlparser.AndExpr) (*sqlparser.AndExpr, error) {
	switch col := a.Left.(type) {
	case *sqlparser.ComparisonExpr:
		colName := col.Right.(*sqlparser.ColName).Name.String()
		q.processColumn(rightTable, colName)
	case *sqlparser.AndExpr:
		return q.joinConditionFlatten(rightTable, col)
	}
	return nil, nil
}

func processColumn(tableName string, columnName string) error {
	colHashKey := fmt.Sprintf("%s.%s", tableName, columnName)
	_, ok := q.Columns[colHashKey]
	if ok {
		return errors.New("column already exists")
	}

	column := Column{
		Table:    &tableName,
		Position: len(q.Columns) + 1,
	}

	q.Columns[colHashKey] = column

	return nil
}
