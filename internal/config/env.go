package config

import (
	"fmt"
	"os/user"
	"path/filepath"
)

type Env struct {
	HyphaConfigPath string
	LicenseKey      string
}

func EnvInit() (*Env, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %w", err)
	}

	return &Env{
		HyphaConfigPath: getEnv("HYPHADB_CONFIG_PATH", filepath.Join(usr.HomeDir, ".hyphadb"), false),
		LicenseKey:      getEnv("HYPHADB_LICENSE_KEY", "", true),
	}, nil
}
