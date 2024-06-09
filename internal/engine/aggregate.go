package engine

func (q *Query) AggregateReducer() error {
	var results []map[string]any
	for _, node := range q.Nodes {
		for _, result := range node.NodeResults {
			for _, row := range result {
				if len(results) == 0 {
					results = append(results, row)
				} else {
					q.ResultsUpdate(results, row)
				}
			}
		}
	}

	q.Results <- results[0]

	return nil
}

func (q *Query) ResultsUpdate(results []map[string]any, row map[string]any) {
	for idx := range results {
		for col, val := range row {
			switch q.Columns[col].FuncName {
			case "sum":
				results[idx][col] = results[idx][col].(int64) + val.(int64)
			case "count":
				results[idx][col] = results[idx][col].(int64) + val.(int64)
			case "min":
				if results[idx][col].(int32) > val.(int32) {
					results[idx][col] = val
				}
			case "max":
				if results[idx][col].(int32) < val.(int32) {
					results[idx][col] = val
				}
			}
		}
	}
}
