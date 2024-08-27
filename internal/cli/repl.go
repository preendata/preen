package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/chzyer/readline"
	"github.com/hyphasql/hypha/internal/config"
	"github.com/hyphasql/hypha/internal/engine"
	"github.com/hyphasql/hypha/internal/utils"
	"github.com/urfave/cli/v2"
)

func Repl(c *cli.Context) error {
	conf, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("error getting config: %w", err)
	}

	outputFormat := c.String("output-format")
	fmt.Println("Output format: ", outputFormat)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            "hypha> ",
		HistoryFile:       "/tmp/hypha-history.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer rl.Close()

	fmt.Println("REPL started. Type 'exit' to quit.")
	var cmds []string
	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		line = strings.TrimSpace(line)

		// Handle exit command
		if line == "exit" || line == "quit" {
			fmt.Println("Exiting REPL.")
			break
		}

		cmds = append(cmds, line)
		if !strings.HasSuffix(line, ";") {
			rl.SetPrompt(">")
			continue
		}

		cmd := strings.Join(cmds, " ")
		cmds = cmds[:0]
		rl.SetPrompt("hyphadb> ")
		rl.SaveHistory(cmd)

		// Execute the input as a query
		qr, err := engine.Execute(cmd, conf)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		utils.WriteToTable(qr.Rows, qr.Columns, outputFormat)
	}

	return nil
}
