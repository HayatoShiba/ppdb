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

func TestReleaseHeaderLock(t *testing.T) {
	var lockedState uint32 = 0x200
	desc := &descriptor{
		state: lockedState,
	}
	assert.False(t, desc.state&bmLocked == 0)
	desc.releaseHeaderLock()
	assert.True(t, desc.state&bmLocked == 0)
}

func TestAcquireHeaderLock(t *testing.T) {
	var unlockedState uint32 = 0x0
	desc := &descriptor{
		state: unlockedState,
	}
	assert.True(t, desc.state&bmLocked == 0)
	desc.acquireHeaderLock()
	assert.False(t, desc.state&bmLocked == 0)
}

func TestDirty(t *testing.T) {
	var undirtystate uint32 = 0x0
	desc := &descriptor{
		state: undirtystate,
	}
	assert.False(t, desc.isDirty())
	desc.setDirty()
	assert.True(t, desc.isDirty())
	// check setDirty is no problem when dirty bit is on
	desc.setDirty()
	desc.clearDirty()
	assert.False(t, desc.isDirty())
	// check clearDirty is no problem when dirty bit is off
	desc.clearDirty()
}

func TestIOInProgress(t *testing.T) {
	var inProgressState uint32 = 0x80
	desc := &descriptor{
		state: inProgressState,
	}
	assert.False(t, desc.state&bmIOInProgress == 0)
	desc.clearIOInProgress()
	assert.True(t, desc.state&bmIOInProgress == 0)
	// check clearIOInProgress is no problem when the bit is off
	desc.clearIOInProgress()
}
