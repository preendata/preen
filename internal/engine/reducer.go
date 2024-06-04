package engine

func (q *Query) Reduce() error {
	if q.Nodes[0].JoinDetails.JoinExpr != nil {
		q.JoinReducer()
	}

	if q.ReducerRequired && q.IsAggregate {
		q.AggregateReducer()
	}

	return nil
}
