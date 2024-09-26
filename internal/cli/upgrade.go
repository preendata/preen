package cli

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

func Upgrade() error {
	// Define the install script URL
	installScriptURL := "https://raw.githubusercontent.com/hyphasql/hypha/main/build/install.sh"

	// Determine the command to use for downloading the script
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("powershell", "-Command", "Invoke-WebRequest -Uri", installScriptURL, "-OutFile install.sh; ./install.sh")
	} else {
		cmd = exec.Command("sh", "-c", fmt.Sprintf("curl -fsSL %s | sh", installScriptURL))
	}

	// Set the command's output to the standard output and error
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run the command
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to upgrade: %w", err)
	}

	fmt.Println("Hypha has been successfully upgraded.")
	return nil
}
