package vm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsAllVisible(t *testing.T) {
	tests := []struct {
		name     string
		flags    uint8
		expected bool
	}{
		{
			name:     "the status is all visible",
			flags:    StatusAllVisible,
			expected: true,
		},
		{
			name:     "the status is initialized",
			flags:    StatusInitialized,
			expected: false,
		},
		{
			name:     "the status is frozen",
			flags:    StatusAllFrozen,
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsAllVisible(tt.flags)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIsAllFrozen(t *testing.T) {
	tests := []struct {
		name     string
		flags    uint8
		expected bool
	}{
		{
			name:     "the status is all visible",
			flags:    StatusAllVisible,
			expected: false,
		},
		{
			name:     "the status is initialized",
			flags:    StatusInitialized,
			expected: false,
		},
		{
			name:     "the status is frozen",
			flags:    StatusAllFrozen,
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsAllFrozen(tt.flags)
			assert.Equal(t, tt.expected, got)
		})
	}
}
