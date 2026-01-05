package logger_test

import (
	"bytes"
	"encoding/json"
	"froopydb/logger"
	"testing"
)

func TestLoggerLevelsDebugShouldLog(t *testing.T) {
	var output bytes.Buffer
	jsond := json.NewDecoder(&output)

	logger := logger.NewLogger(logger.DEBUG)
	logger.SetOutput(&output)

	logger.Debug("This is a debug message", "key1", "value1")

	var logMsg map[string]any
	if err := jsond.Decode(&logMsg); err != nil {
		t.Fatalf("failed to decode log message: %v", err)
	}
	if logMsg["level"] != "DEBUG" {
		t.Fatalf("expected level DEBUG, got %v", logMsg["level"])
	}
	if logMsg["message"] != "This is a debug message" {
		t.Fatalf("expected message 'This is a debug message', got %v", logMsg["message"])
	}
	if logMsg["key1"] != "value1" {
		t.Fatalf("expected key1 to be 'value1', got %v", logMsg["key1"])
	}
	output.Reset()

	logger.Info("This is an info message", "key2", "value2")
	if err := jsond.Decode(&logMsg); err != nil {
		t.Fatalf("failed to decode log message: %v", err)
	}
	if logMsg["level"] != "INFO" {
		t.Fatalf("expected level INFO, got %v", logMsg["level"])
	}
	if logMsg["message"] != "This is an info message" {
		t.Fatalf("expected message 'This is an info message', got %v", logMsg["message"])
	}
	if logMsg["key2"] != "value2" {
		t.Fatalf("expected key2 to be 'value2', got %v", logMsg["key2"])
	}
}

func TestLoggerLevelsInfoShouldNotLog(t *testing.T) {
	var output bytes.Buffer
	logger := logger.NewLogger(logger.INFO)
	logger.SetOutput(&output)

	logger.Debug("This is a debug message", "key1", "value1")

	if output.Len() != 0 {
		t.Fatalf("expected no output for DEBUG level when logger level is INFO, got %s", output.String())
	}

	logger.Info("This is an info message", "key2", "value2")
	if output.Len() == 0 {
		t.Fatalf("expected output for INFO level when logger level is INFO, got none")
	}
	output.Reset()

	logger.Warning("This is a warn message", "key3", "value3")
	if output.Len() == 0 {
		t.Fatalf("expected output for WARN level when logger level is INFO, got none")
	}
	output.Reset()

	logger.Error("This is an error message", "key4", "value4")
	if output.Len() == 0 {
		t.Fatalf("expected output for ERROR level when logger level is INFO, got none")
	}
}

func TestLoggerLevelsErrorShouldLogOnlyError(t *testing.T) {
	var output bytes.Buffer
	logger := logger.NewLogger(logger.ERROR)
	logger.SetOutput(&output)

	logger.Debug("This is a debug message", "key1", "value1")
	logger.Info("This is an info message", "key2", "value2")
	logger.Warning("This is a warn message", "key3", "value3")

	if output.Len() != 0 {
		t.Fatalf("expected no output for DEBUG, INFO, WARN levels when logger level is ERROR, got %s", output.String())
	}

	logger.Error("This is an error message", "key4", "value4")
	if output.Len() == 0 {
		t.Fatalf("expected output for ERROR level when logger level is ERROR, got none")
	}
}
