package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

func doSomething(c *cli.Context) error {
	fmt.Println("Doing something useful...")
	return nil
}
