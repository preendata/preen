package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
)

type Env struct {
	HyphaConfigPath string
	HyphaModelPath  string
	LicenseKey      string
}

func EnvInit() (*Env, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %w", err)
	}

	workDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current working directory: %w", err)
	}

	return &Env{
		HyphaConfigPath: getEnv("HYPHA_CONFIG_PATH", filepath.Join(usr.HomeDir, ".hypha"), false),
		HyphaModelPath:  getEnv("HYPHA_MODEL_PATH", filepath.Join(workDir, "models"), false),
		LicenseKey:      getEnv("HYPHA_LICENSE_KEY", "", false),
	}, nil
}
