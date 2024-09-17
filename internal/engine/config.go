package engine

func GetConfig() (*SourceConfig, *ModelConfig, error) {
	sc, err := GetSourceConfig()
	if err != nil {
		return nil, nil, err
	}

	mc, err := GetModelConfigs()
	if err != nil {
		return nil, nil, err
	}

	return sc, mc, nil
}
