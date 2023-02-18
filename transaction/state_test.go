package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCompleted(t *testing.T) {
	tests := []struct {
		name     string
		state    State
		expected bool
	}{
		{
			name:     "aborted",
			state:    StateAborted,
			expected: true,
		},
		{
			name:     "committed",
			state:    StateCommitted,
			expected: true,
		},
		{
			name:     "in progress",
			state:    StateInProgress,
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsCompleted(tt.state)
			assert.Equal(t, tt.expected, got)
		})
	}
}
