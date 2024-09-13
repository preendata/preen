package engine

import (
	"os"
	"testing"
)

func TestGetEnv(t *testing.T) {
	// Set up the environment variable
	os.Setenv("TEST_KEY", "test_value")
	defer os.Unsetenv("TEST_KEY")

	// Test cases
	tests := []struct {
		key          string
		defaultValue string
		expected     string
		required     bool
	}{
		{"TEST_KEY", "default_value", "test_value", true},
		{"NON_EXISTENT_KEY", "default_value", "default_value", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := getEnv(tt.key, tt.defaultValue, tt.required)
			if result != tt.expected {
				t.Errorf("GetEnv(%s, %s) = %s; want %s", tt.key, tt.defaultValue, result, tt.expected)
			}
		})
	}
}
