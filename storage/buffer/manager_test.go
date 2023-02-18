package buffer

import (
	"bytes"
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestFlushBuffer(t *testing.T) {
	m, err := TestingNewManager()
	assert.Nil(t, err)

	var bufID BufferID = 3
	// set descriptor for test
	m.descriptors[bufID] = &descriptor{
		tag: *newTag(common.Relation(1), disk.ForkNumberMain, page.PageID(3)),
	}
	// set buffer content for test
	rp, err := page.TestingNewRandomPage()
	assert.Nil(t, err)
	m.buffers[bufID] = buffer(rp)
	err = m.flushBuffer(bufID)
	assert.Nil(t, err)

	// check whether the content is flushed to disk
	flushed := page.NewPagePtr()
	err = m.dm.ReadPage(common.Relation(1), disk.ForkNumberMain, page.PageID(3), flushed)
	assert.Nil(t, err)

	assert.True(t, bytes.Equal(flushed[:], rp[:]))
}

func TestReadBuffer(t *testing.T) {
	t.Run("when page.NewPageID is specified to extend the page", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		rel := common.Relation(1)
		forkNum := disk.ForkNumberMain
		npid, err := m.dm.GetNPageID(rel, forkNum)
		assert.Nil(t, err)

		// extend page
		bufID, err := m.ReadBuffer(rel, forkNum, page.NewPageID)
		assert.Nil(t, err)
		// check the buffer descriptor for whether the page is extended or not
		assert.Equal(t, npid+1, m.descriptors[bufID].tag.pageID)
		// the fetched page has not been initialized
		assert.False(t, page.IsInitialized(m.GetPage(bufID)))
	})
	t.Run("when the page id is already stored in buffer table", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		rel := common.Relation(1)
		forkNum := disk.ForkNumberMain

		// extend, and write the random contents to the page. (to check whether the contents is fetched into buffer)
		npid, err := m.dm.ExtendPage(rel, forkNum, false)
		assert.Nil(t, err)
		p, err := page.TestingNewRandomPage()
		assert.Nil(t, err)
		err = m.dm.WritePage(rel, forkNum, npid, p, false)
		assert.Nil(t, err)

		// read npid pageID into buffer
		bufID1, err := m.ReadBuffer(rel, forkNum, npid)
		assert.Nil(t, err)

		// re-read npid pageID into buffer. npid has been already stored in buffer table. so buffer id must be equal.
		bufID2, err := m.ReadBuffer(rel, forkNum, npid)
		assert.Nil(t, err)

		// check whether the same buffer is returned (new buffer should not be allocated for the same page)
		assert.Equal(t, bufID1, bufID2)
		// check whether the page content is actually fetched.
		assert.True(t, bytes.Equal(m.GetPage(bufID2)[:], p[:]))
	})

	t.Run("when dirty buffer allocated from clock-sweep", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		rel := common.Relation(1)
		forkNum := disk.ForkNumberMain

		// この辺りとかmock allocator作って、dirtyにしたbuffer idを返せるようにしたりすると良いのかもしれない
		m.freeList = freeListInvalidID

		// extend page
		// the buffer must be allocated from free list and the buffer id must be FirstBufferID
		bufID, err := m.ReadBuffer(rel, forkNum, page.NewPageID)
		assert.Nil(t, err)

		// write some content to the page in the buffer and turn dirty bit on
		rp, err := page.TestingNewRandomPage()
		assert.Nil(t, err)
		m.buffers[bufID] = buffer(rp)
		// turn dirty bit on
		m.descriptors[bufID].setDirty()
		// persist the content to check later
		expected := make([]byte, len(rp))
		copy(expected, rp[:])

		// the dirty buffer should be evicted for test
		// so change nextVictimBuffer. this leads to the dirty buffer eviction next time
		m.nextVictimBuffer = 0
		// and reset reference count and usage count for the buffer to be evicted
		m.ReleaseBuffer(bufID)
		m.descriptors[bufID].decrementUsageCount()

		// persist the dirty buffer page id to re-fetch later from disk
		pageID := m.descriptors[bufID].tag.pageID

		// read another page from buffer
		// this leads to clock-sweep eviction
		// the dirty buffer is written out to disk
		bufID2, err := m.ReadBuffer(rel, forkNum, page.NewPageID)
		assert.Nil(t, err)
		// check the same buffer id is returned (this means eviction happen (probably))
		assert.Equal(t, bufID, bufID2)

		// check whether the dirty page is written to disk
		flushed := page.NewPagePtr()
		err = m.dm.ReadPage(rel, forkNum, pageID, flushed)
		assert.Nil(t, err)
		assert.True(t, bytes.Equal(flushed[:], expected[:]))

		// the page newly fetched must be 0-filled because the page has not been initialized
		newp := page.PagePtr(m.buffers[bufID2][:])
		assert.False(t, page.IsInitialized(newp))
		// then re-fetch the page whose content was updated with the random byte
		bufID3, err := m.ReadBuffer(rel, forkNum, pageID)
		assert.Nil(t, err)
		// check whether the page fetched has random bytes
		assert.True(t, bytes.Equal(m.buffers[bufID3][:], expected[:]))
	})
}

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
