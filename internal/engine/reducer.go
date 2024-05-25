package engine

func (q *Query) Reduce() (*Query, error) {
	if q.Nodes[0].JoinDetails.JoinExpr != nil {
		q.JoinReducer()
	}
	return q, nil
}
