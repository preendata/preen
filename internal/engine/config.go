package engine

import "fmt"

const Version = "v0.1.1"

func GetConfig(modelTarget string) (*SourceConfig, *ModelConfig, error) {
	sc, err := GetSourceConfig()
	if err != nil {
		return nil, nil, err
	}

	mc, err := GetModelConfigs(modelTarget)
	if err != nil {
		return nil, nil, err
	}

	return sc, mc, nil
}

func ValidateConfigs(sc *SourceConfig, mc *ModelConfig) error {
	if err := errorOnMissingModels(sc, mc); err != nil {
		return fmt.Errorf("error on missing models: %w", err)
	}

	if err := parseModels(mc); err != nil {
		return fmt.Errorf("error parsing models: %w", err)
	}

	return nil
}
