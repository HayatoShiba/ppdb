package fsm

import (
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestUpdateFSM(t *testing.T) {
	m, err := TestingNewManager()
	assert.Nil(t, err)

	tests := []struct {
		name   string
		rel    common.Relation
		pageID page.PageID
		size   int
	}{
		{
			name:   "pattern 1",
			rel:    0,
			pageID: 10,
			size:   90,
		},
		{
			name:   "pattern 2",
			rel:    0,
			pageID: 1000,
			size:   8191,
		},
		{
			name:   "pattern 2",
			rel:    10,
			pageID: 100,
			size:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = m.UpdateFSM(tt.rel, tt.pageID, tt.size)
			assert.Nil(t, err)
		})
	}
}

func TestSearchPageIDWithFreeSpaceSize(t *testing.T) {
	t.Run("when no free space", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		pageID, err := m.SearchPageIDWithFreeSpaceSize(common.Relation(10), 100)
		assert.Nil(t, err)
		assert.Equal(t, page.InvalidPageID, pageID)
	})
	t.Run("when there is enough free space", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		rel := common.Relation(10)
		expectedPageID := page.PageID(0)
		size := 99

		err = m.UpdateFSM(rel, expectedPageID, size)
		assert.Nil(t, err)

		pageID, err := m.SearchPageIDWithFreeSpaceSize(rel, size)
		assert.Nil(t, err)
		assert.Equal(t, expectedPageID, pageID)
	})
	t.Run("when there is multiple enough free space", func(t *testing.T) {
		m, err := TestingNewManager()
		assert.Nil(t, err)

		rel := common.Relation(10)
		pid := page.PageID(0)
		size := 99
		err = m.UpdateFSM(rel, pid, size)
		assert.Nil(t, err)

		expectedPageID := page.PageID(100)
		size = 8000
		err = m.UpdateFSM(rel, expectedPageID, size)
		assert.Nil(t, err)

		pid = page.PageID(80)
		size = 10
		err = m.UpdateFSM(rel, pid, size)
		assert.Nil(t, err)

		pageID, err := m.SearchPageIDWithFreeSpaceSize(rel, 7000)
		assert.Nil(t, err)
		assert.Equal(t, expectedPageID, pageID)
	})
}
