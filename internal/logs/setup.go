package logs

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// Setup configures the process-wide slog logger. Records go to stderr and to
// the Web UI ring buffer (when non-nil), and optionally to an appended file.
//   - level:  "debug" | "info" | "warn" | "error" (default "info")
//   - format: "text" | "json" (default "text")
func Setup(level, format, filePath string, ring io.Writer) {
	var w io.Writer = os.Stderr
	if ring != nil {
		w = io.MultiWriter(os.Stderr, ring)
	}
	if filePath != "" {
		if f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			w = io.MultiWriter(w, f)
		}
	}
	opts := &slog.HandlerOptions{Level: ParseLevel(level)}
	var h slog.Handler
	if strings.EqualFold(format, "json") {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	slog.SetDefault(slog.New(h))

	applyMu.Lock()
	currentW = w
	currentL = ParseLevel(level)
	currentFm = format
	applyMu.Unlock()
}

// ParseLevel maps a config string to a slog.Level (default info).
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Apply reconfigures the running logger (level/format) without a restart.
// No-op when Setup was never called.
var (
	applyMu   sync.Mutex
	currentW  io.Writer
	currentL  = slog.LevelInfo
	currentFm = "text"
)

func Apply(level, format string) {
	applyMu.Lock()
	defer applyMu.Unlock()
	if currentW == nil {
		return
	}
	if level != "" {
		currentL = ParseLevel(level)
	}
	if format != "" {
		currentFm = format
	}
	var h slog.Handler
	opts := &slog.HandlerOptions{Level: currentL}
	if strings.EqualFold(currentFm, "json") {
		h = slog.NewJSONHandler(currentW, opts)
	} else {
		h = slog.NewTextHandler(currentW, opts)
	}
	slog.SetDefault(slog.New(h))
}
