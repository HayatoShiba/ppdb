package clog

import (
	"bytes"
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestWriteReadPage(t *testing.T) {
	dm, err := TestingNewDiskManager(t)
	assert.Nil(t, err)

	var expected page.PagePtr
	expected = &[page.PageSize]byte{'g', 'a'}

	err = dm.writePage(page.FirstPageID, expected)
	assert.Nil(t, err)

	got := page.NewPagePtr()
	err = dm.readPage(page.FirstPageID, got)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(got[:], expected[:]))
}
