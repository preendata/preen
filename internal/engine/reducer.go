package engine

func (q *Query) Reduce() (*Query, error) {
	for idx := range q.Nodes {
		q.Results = append(q.Results, q.Nodes[idx].NodeResults...)
	}

	return q, nil
}
