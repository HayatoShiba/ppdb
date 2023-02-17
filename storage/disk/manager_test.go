package disk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	baseDir = t.TempDir()
	_, err := NewManager()
	assert.Nil(t, err)
}

func TestReadPage(t *testing.T) {
	dm, err := TestingNewManager(t)
	assert.Nil(t, err)

	expected := [page.PageSize]byte{'g', 'a'}
	// override the getDatabaseFile function for test
	getDatabaseFile = func() *os.File {
		path := filepath.Join(baseDir, "sampleTableFile")
		f, err := os.Create(path)
		assert.Nil(t, err)
		_, err = f.Write(expected[:])
		assert.Nil(t, err)
		return f
	}

	got := page.NewPagePtr()
	err = dm.ReadPage(page.FirstPageID, got)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(got[:], expected[:]))
}

func TestWritePage(t *testing.T) {
	dm, err := TestingNewManager(t)
	assert.Nil(t, err)

	path := filepath.Join(baseDir, "sampleTableFile")
	// override the getDatabaseFile function for test
	getDatabaseFile = func() *os.File {
		f, err := os.Create(path)
		assert.Nil(t, err)
		return f
	}

	expected := [page.PageSize]byte{'g', 'a'}
	err = dm.WritePage(page.FirstPageID, page.PagePtr(&expected), false)
	assert.Nil(t, err)

	got, err := os.ReadFile(path)
	assert.Nil(t, err)

	// check the equality of page
	assert.True(t, bytes.Equal(got[:], expected[:]))
}
