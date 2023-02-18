package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClockSweepTick(t *testing.T) {
	m, err := TestingNewManager()
	assert.Nil(t, err)
	bufferID := m.clockSweepTick()
	assert.Equal(t, FirstBufferID+1, bufferID)
	bufferID = m.clockSweepTick()
	assert.Equal(t, FirstBufferID+2, bufferID)
}

func TestAllocateWithClockSweep(t *testing.T) {
	t.Run("without pin", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)
		bufferID := m.allocateWithClockSweep()
		assert.Equal(t, FirstBufferID+1, bufferID)
		bufferID = m.allocateWithClockSweep()
		assert.Equal(t, FirstBufferID+2, bufferID)
	})
	t.Run("when pinned without unpin", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)
		victim := FirstBufferID + 1
		desc := m.descriptors[victim]
		desc.pin()
		bufferID := m.allocateWithClockSweep()
		// victim must not be evicted
		assert.Equal(t, victim+1, bufferID)
	})
	t.Run("when unpinned after pinned", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)
		victim := FirstBufferID + 1
		desc := m.descriptors[victim]
		desc.pin()
		desc.unpin()
		assert.Equal(t, uint32(1), desc.usageCount())
		bufferID := m.allocateWithClockSweep()
		// victim must not be evicted
		assert.Equal(t, victim+1, bufferID)
		// usage count must be decremented
		assert.Equal(t, uint32(0), desc.usageCount())
	})
}
