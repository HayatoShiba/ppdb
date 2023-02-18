package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocateBuffer(t *testing.T) {
	m, err := TestingNewManagerWithOneElementInFreeList()
	assert.Nil(t, err)
	desc := m.descriptors[FirstBufferID+2]
	desc.pin()
	tests := []struct {
		name     string
		expected BufferID
	}{
		{
			name:     "allocation first time: this buffer is from free list",
			expected: FirstBufferID,
		},
		{
			name:     "allocation second time: this buffer is from clock sweep",
			expected: FirstBufferID + 1,
		},
		{
			name:     "allocation third time: this buffer is from clock sweep. FirstBufferID+2 must be skipped",
			expected: FirstBufferID + 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.allocateBuffer()
			assert.Equal(t, tt.expected, got)
			assert.Nil(t, err)
		})
	}
}
