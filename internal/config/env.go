package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
)

type Env struct {
	HyphaConfigPath  string
	HyphaContextPath string
	LicenseKey       string
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
		HyphaConfigPath:  getEnv("HYPHADB_CONFIG_PATH", filepath.Join(usr.HomeDir, ".hyphadb"), false),
		HyphaContextPath: getEnv("HYPHADB_CONTEXT_PATH", filepath.Join(workDir, "contexts"), false),
		LicenseKey:       getEnv("HYPHADB_LICENSE_KEY", "", true),
	}, nil
}
