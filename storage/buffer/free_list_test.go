package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocateBufferFromFreeList(t *testing.T) {
	m, err := TestingNewManager()
	assert.Nil(t, err)
	got := m.allocateFromFreeList()
	assert.Equal(t, FirstBufferID, got)
}

func TestAllocateBufferFromFreeList_NoFreeList(t *testing.T) {
	m, err := TestingNewManagerWithNoFreeList()
	assert.Nil(t, err)
	got := m.allocateFromFreeList()
	assert.Equal(t, freeListInvalidID, got)
}

func TestAllocateBufferFromFreeList_OneElementInFreeList(t *testing.T) {
	m, err := TestingNewManagerWithOneElementInFreeList()
	assert.Nil(t, err)
	tests := []struct {
		name     string
		expected BufferID
	}{
		{
			name:     "allocation first time",
			expected: FirstBufferID,
		},
		{
			name:     "allocation second time",
			expected: freeListInvalidID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.allocateFromFreeList()
			assert.Equal(t, tt.expected, got)
		})
	}
}
