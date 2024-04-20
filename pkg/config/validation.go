package config

type Database struct {
	TableResults map[string]TableResult `json:"table_result"`
	Url          string                 `json:"url"`
}

type TableResult struct {
	Columns map[string]string `json:"columns"`
	Schema  string            `json:"schema"`
}

type ColumnType struct {
	Types        []string `json:"types"`
	MajorityType string   `json:"majority_type"`
}

type Validator struct {
	Databases   map[string]Database              `json:"databases"`
	ColumnTypes map[string]map[string]ColumnType `json:"column_types"`
	Cfg         Config
}
