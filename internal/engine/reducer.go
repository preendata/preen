package engine

func (q *Query) Reduce() error {
	if q.JoinDetails.JoinExpr != nil {
		q.JoinReducer()
	}

	if q.ReducerRequired && q.IsAggregate {
		q.AggregateReducer()
	}

	return nil
}
