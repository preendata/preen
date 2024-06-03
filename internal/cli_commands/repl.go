package cli_commands

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/engine"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
)

func Repl(c *cli.Context) error {
	conf, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("error getting config: %w", err)
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            "> ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer rl.Close()

	fmt.Println("REPL started. Type 'exit' to quit.")
	for {
		input, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(input) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		input = strings.TrimSpace(input)

		fmt.Println("Input received: ", input)

		// Handle exit command
		if input == "exit" || input == "quit" {
			fmt.Println("Exiting REPL.")
			break
		}

		// Execute the input as a query
		result, err := engine.Execute(input, &conf)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Println(result)

		writeToTable(result)
	}

	return nil
}

func writeToTable(rows []map[string]any) {

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)

	// Set table headers
	headers := table.Row{}
	for header := range rows[0] {
		headers = append(headers, header)
	}
	t.AppendHeader(headers)

	// Populate table with data
	for _, row := range rows {
		values := table.Row{}
		for _, header := range headers {
			values = append(values, row[header.(string)])
		}
		t.AppendRow(values)
	}

	t.Render()
}
