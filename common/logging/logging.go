package logging

import (
	"log/slog"
)

type Config struct {
	Verbosity string `koanf:"verbosity"`
}

func SetLevel(cfg Config) {
	loggingLevel := slog.LevelInfo
	switch cfg.Verbosity {
	case "":
		// If no verbosity is given, default to Info.
		loggingLevel = slog.LevelInfo
	case "debug":
		loggingLevel = slog.LevelDebug
	case "info":
		loggingLevel = slog.LevelInfo
	default:
		slog.Warn("Invalid logging verbosity provided, defaulting to Info level", slog.Any("verbosity", cfg.Verbosity))
	}
	slog.SetLogLoggerLevel(loggingLevel)
}
