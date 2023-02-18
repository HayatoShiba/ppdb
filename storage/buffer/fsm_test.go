package buffer

import (
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestReadBufferFSM(t *testing.T) {
	m, err := TestingNewManager()
	assert.Nil(t, err)

	// extend page
	// the buffer must be allocated from free list and the buffer id must be FirstBufferID
	expected := page.PageID(10)
	_, err = m.ReadBufferFSM(common.Relation(1), expected, false)
	assert.Nil(t, err)

	// confirm page has been extended
	npid, err := m.dm.GetNPageID(common.Relation(1), disk.ForkNumberFSM)
	assert.Nil(t, err)
	assert.Equal(t, expected, npid)
}

func TestReleaseBufferFSM(t *testing.T) {
	t.Run("when holding shared content lock", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		expected := page.PageID(10)
		exclusive := false
		bufID, err := m.ReadBufferFSM(common.Relation(1), expected, exclusive)
		assert.Nil(t, err)
		m.ReleaseBufferFSM(bufID, exclusive)
	})
	t.Run("when holding exclusive content lock", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		expected := page.PageID(10)
		exclusive := true
		bufID, err := m.ReadBufferFSM(common.Relation(1), expected, exclusive)
		assert.Nil(t, err)
		m.ReleaseBufferFSM(bufID, exclusive)
	})
}
