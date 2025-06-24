package config

import (
	"fmt"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

func ParseYAML[C any](filePath, configPath string) (C, error) {
	var cfg C

	k := koanf.New(".")
	if err := k.Load(file.Provider(filePath), yaml.Parser()); err != nil {
		return cfg, fmt.Errorf("parsing yaml: %w", err)
	}

	k.Unmarshal(configPath, &cfg)
	return cfg, nil
}
