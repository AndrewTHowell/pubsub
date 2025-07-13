package logging

import (
	"fmt"
	"log/slog"
)

type Config struct {
	Verbosity string `koanf:"verbosity"`
}

func SetLevel(cfg Config) error {
	loggingLevel := slog.LevelInfo
	switch cfg.Verbosity {
	case "debug":
		loggingLevel = slog.LevelDebug
	default:
		return fmt.Errorf("invalid logging verbosity: %q", cfg.Verbosity)
	}
	slog.SetLogLoggerLevel(loggingLevel)
	return nil
}
