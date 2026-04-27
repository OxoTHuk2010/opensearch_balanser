package observability

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

type Logger struct {
	mu       sync.Mutex
	out      io.Writer
	minLevel string
}

func NewLogger(path, level string) (*Logger, error) {
	out := io.Writer(os.Stdout)
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		out = io.MultiWriter(os.Stdout, f)
	}
	if level == "" {
		level = "info"
	}
	return &Logger{out: out, minLevel: level}, nil
}

func (l *Logger) Info(msg string, fields map[string]any) {
	l.log("info", msg, fields)
}

func (l *Logger) Error(msg string, fields map[string]any) {
	l.log("error", msg, fields)
}

func (l *Logger) log(level, msg string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	fields["level"] = level
	fields["msg"] = msg
	b, _ := json.Marshal(fields)
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(b, '\n'))
}
