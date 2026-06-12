// Package pkgcluster provides a pluggable cluster membership system inspired by
// libcluster (https://github.com/bitwalker/libcluster). It discovers cluster
// nodes through configurable strategies and delegates connect/disconnect
// decisions to the caller via callbacks.
package pkgcluster

import (
	"log"
	"os"
)

// Logger defines the logging interface used by pkgcluster strategies.
// Strategies call the appropriate level method for diagnostic messages.
type Logger struct {
	Debug func(msg string, args ...any)
	Info  func(msg string, args ...any)
	Warn  func(msg string, args ...any)
	Error func(msg string, args ...any)
}

// defaultLogger logs to stderr via log.Printf with a [pkgcluster] prefix.
var defaultLogger = Logger{
	Debug: func(msg string, args ...any) {
		log.Printf("[pkgcluster:DEBUG] "+msg, args...)
	},
	Info: func(msg string, args ...any) {
		log.Printf("[pkgcluster:INFO] "+msg, args...)
	},
	Warn: func(msg string, args ...any) {
		log.Printf("[pkgcluster:WARN] "+msg, args...)
	},
	Error: func(msg string, args ...any) {
		log.Printf("[pkgcluster:ERROR] "+msg, args...)
	},
}

// SetLogger replaces the global logger used by strategies. Call it once
// during initialisation to wire in your application's logger.
//
// Example using log/slog:
//
//	pkgcluster.SetLogger(pkgcluster.Logger{
//	    Debug: func(msg string, args ...any) { slog.Debug(msg, args...) },
//	    Info:  func(msg string, args ...any) { slog.Info(msg, args...) },
//	    Warn:  func(msg string, args ...any) { slog.Warn(msg, args...) },
//	    Error: func(msg string, args ...any) { slog.Error(msg, args...) },
//	})
func SetLogger(l Logger) {
	globalLogger = l
}

// initLogger is called once at startup to set the global logger.
// It respects the PKGCLUSTER_LOG env var for debug output.
var globalLogger = func() Logger {
	if os.Getenv("PKGCLUSTER_LOG") != "" {
		return defaultLogger
	}
	return Logger{
		Debug: func(string, ...any) {}, // no-op
		Info:  defaultLogger.Info,
		Warn:  defaultLogger.Warn,
		Error: defaultLogger.Error,
	}
}()
