package engine

type QueryResults struct {
	Rows        []map[string]any
	Columns     []string
	ResultsChan chan map[string]any
}

var err error

func Execute(statement string) (*QueryResults, error) {
	Debug("Executing query: " + statement)
	qr := QueryResults{
		ResultsChan: make(chan map[string]any),
	}

	go qr.collectResults(qr.ResultsChan)

	qr.Columns, err = ddbQuery(statement, qr.ResultsChan)
	if err != nil {
		return nil, err
	}

	return &qr, nil
}

func (qr *QueryResults) collectResults(c chan map[string]any) error {
	for row := range c {
		qr.Rows = append(qr.Rows, row)
	}
	return nil
}
