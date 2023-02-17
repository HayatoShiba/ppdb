package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferDescriptors(t *testing.T) {
	descs := newDescriptors()
	tests := []struct {
		name     string
		id       int
		expected BufferID
	}{
		{
			name:     "id is 10",
			id:       10,
			expected: 11,
		},
		{
			name:     "id is bufferNum-1",
			id:       bufferNum - 1,
			expected: freeListInvalidID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := descs[tt.id].nextFreeID
			assert.Equal(t, tt.expected, got)
		})
	}
}
