package disk

import (
	"bytes"
	"os"
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	baseDir = t.TempDir()
	_, err := NewManager()
	assert.Nil(t, err)
}

func TestReadPage(t *testing.T) {
	dm, err := TestingNewFileManager(t)
	assert.Nil(t, err)

	// create test file
	rel := common.Relation(1)
	path := getRelationForkFilePath(rel, ForkNumberMain)
	f, err := os.Create(path)
	assert.Nil(t, err)

	expected := [page.PageSize]byte{'g', 'a'}
	_, err = f.Write(expected[:])
	assert.Nil(t, err)

	got := page.NewPagePtr()
	err = dm.ReadPage(rel, ForkNumberMain, page.FirstPageID, got)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(got[:], expected[:]))
}

func TestWritePage(t *testing.T) {
	dm, err := TestingNewFileManager(t)
	assert.Nil(t, err)

	rel := common.Relation(1)
	expected := [page.PageSize]byte{'g', 'a'}
	err = dm.WritePage(rel, ForkNumberMain, page.FirstPageID, page.PagePtr(&expected), false)
	assert.Nil(t, err)

	path := getRelationForkFilePath(rel, ForkNumberMain)
	got, err := os.ReadFile(path)
	assert.Nil(t, err)

	// check the equality of page
	assert.True(t, bytes.Equal(got[:], expected[:]))
}

func TestExtendPage(t *testing.T) {
	dm, err := TestingNewFileManager(t)
	assert.Nil(t, err)

	// create test file
	rel := common.Relation(1)
	path := getRelationForkFilePath(rel, ForkNumberMain)
	f, err := os.Create(path)
	assert.Nil(t, err)

	temp := [page.PageSize]byte{'g', 'a'}

	nPageID := 2
	for i := 0; i <= nPageID; i++ {
		_, err := f.Write(temp[:])
		assert.Nil(t, err)
	}

	expected := page.PageID(nPageID + 1)
	got, err := dm.ExtendPage(rel, ForkNumberMain, false)
	assert.Nil(t, err)
	assert.Equal(t, expected, got)
}

func TestGetNPageID(t *testing.T) {
	dm, err := TestingNewFileManager(t)
	assert.Nil(t, err)

	tests := []struct {
		name     string
		nPageID  page.PageID
		expected page.PageID
	}{
		{
			name:     "file size is 0",
			nPageID:  0,
			expected: page.InvalidPageID,
		},
		{
			name:     "file size is 1",
			nPageID:  1,
			expected: page.FirstPageID,
		},
		{
			name:     "file size is 2",
			nPageID:  2,
			expected: page.FirstPageID + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create test file
			rel := common.Relation(1)
			path := getRelationForkFilePath(rel, ForkNumberMain)
			f, err := os.Create(path)
			assert.Nil(t, err)

			temp := [page.PageSize]byte{'g', 'a'}

			for i := page.PageID(0); i < tt.nPageID; i++ {
				_, err := f.Write(temp[:])
				assert.Nil(t, err)
			}
			got, err := dm.GetNPageID(rel, ForkNumberMain)
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}
