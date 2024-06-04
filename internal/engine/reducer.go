package engine

func (q *Query) Reduce() error {
	if q.Nodes[0].JoinDetails.JoinExpr != nil {
		q.JoinReducer()
	}
	return nil
}
