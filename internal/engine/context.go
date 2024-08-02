package engine

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/xwb1989/sqlparser"
)

type ContextQuery struct {
	Query     string
	Parsed    sqlparser.Statement
	DDLString string
	Columns   map[string]map[string]Column
}

type Context struct {
	ContextQueries map[string]ContextQuery
}

func BuildContext(cfg *config.Config) error {
	validator, err := pg.Validate(cfg)
	if err != nil {
		return fmt.Errorf("error validating data sources: %w", err)
	}

	context := Context{}

	utils.Debug("Building context")
	context.ContextQueries, err = readContextFiles(cfg)
	if err != nil {
		return fmt.Errorf("error reading context files: %w", err)
	}

	context.ContextQueries, err = ParseContextColumns(context.ContextQueries, *validator)
	if err != nil {
		return fmt.Errorf("error parsing columns: %w", err)
	}

	connector, err := duckdb.CreateConnector()
	if err != nil {
		return err
	}

	db, err := duckdb.OpenDatabase(connector)
	if err != nil {
		return err
	}
	defer db.Close()

	err = buildTables(db, context.ContextQueries)
	if err != nil {
		return fmt.Errorf("error building context tables: %w", err)
	}

	err = Retrieve(cfg, context)
	if err != nil {
		return fmt.Errorf("error retrieving data: %w", err)
	}

	return nil
}

func readContextFiles(cfg *config.Config) (map[string]ContextQuery, error) {
	contextQueries := make(map[string]ContextQuery, 0)
	files, err := os.ReadDir(cfg.Env.HyphaConfigPath)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".context.sql") {
			utils.Debug("Loading ", file.Name())
			bytes, err := os.ReadFile(cfg.Env.HyphaConfigPath + "/" + file.Name())
			if err != nil {
				return nil, err
			}
			cq := ContextQuery{
				Query: string(bytes),
			}
			utils.Debug("Parsing query: ", cq.Query)
			parsedQuery, err := sqlparser.Parse(cq.Query)
			if err != nil {
				return nil, err
			}
			cq.Parsed = parsedQuery
			contextQueries[strings.TrimSuffix(file.Name(), ".context.sql")] = cq
		}
	}

	return contextQueries, nil
}

func buildTables(db *sql.DB, contextQueries map[string]ContextQuery) error {
	for contextName, contextQuery := range contextQueries {
		dropTable := fmt.Sprintf("drop table if exists main.%s;", contextName)
		_, err := db.Exec(dropTable)
		if err != nil {
			return err
		}
		createTable := fmt.Sprintf("create table main.%s (%s);", contextName, contextQuery.DDLString)
		_, err = db.Exec(createTable)
		if err != nil {
			return err
		}
	}
	return nil
}
