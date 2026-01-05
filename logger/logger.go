package logger

import (
	"encoding/json"
	"io"
	"os"
	"time"
)

const (
	DEBUG = iota
	INFO
	WARN
	ERROR
)

var levelNames = map[int]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
}

type Logger struct {
	Level  int
	output io.Writer
}

func NewLogger(level int) *Logger {
	return &Logger{Level: level, output: os.Stdout}
}

// SetOutput sets the output destination for the logger.
// Could be os.Stderr, bytes.Buffer, or any io.Writer.
func (l *Logger) SetOutput(w io.Writer) {
	l.output = w
}

func (l *Logger) addKvargs(data map[string]any, args ...any) {
	for i := 0; i < len(args)-1; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		data[key] = args[i+1]
	}
}

func (l *Logger) log(level int, msg string, args ...any) {
	jsonMsg := map[string]any{
		"level":   levelNames[level],
		"ts":      time.Now(),
		"message": msg,
	}
	l.addKvargs(jsonMsg, args...)

	if l.Level <= level {
		json.NewEncoder(l.output).Encode(jsonMsg)
	}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.log(DEBUG, msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.log(INFO, msg, args...)
}

func (l *Logger) Warning(msg string, args ...any) {
	l.log(WARN, msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.log(ERROR, msg, args...)
}
