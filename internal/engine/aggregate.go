package engine

func (q *Query) AggregateReducer() error {
	for _, node := range q.Nodes {
		for _, result := range node.NodeResults {
			for _, row := range result {
				if len(q.Results) == 0 {
					q.Results = append(q.Results, row)
				} else {
					q.ResultsUpdate(row)
				}
			}
		}
	}
	return nil
}

func (q *Query) ResultsUpdate(row map[string]any) {
	for idx := range q.Results {
		for col, val := range row {
			switch q.Columns[col].FuncName {
			case "sum":
				q.Results[idx][col] = q.Results[idx][col].(int64) + val.(int64)
			case "count":
				q.Results[idx][col] = q.Results[idx][col].(int64) + val.(int64)
			case "min":
				if q.Results[idx][col].(int32) > val.(int32) {
					q.Results[idx][col] = val
				}
			case "max":
				if q.Results[idx][col].(int32) < val.(int32) {
					q.Results[idx][col] = val
				}
			}
		}
	}
}
