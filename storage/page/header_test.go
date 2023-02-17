package page

import (
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/stretchr/testify/assert"
)

func TestGetAndSetLSN(t *testing.T) {
	page := NewPagePtr()
	var expected common.WALRecordPtr = 100
	SetLSN(page, expected)
	got := GetLSN(page)
	assert.Equal(t, expected, got)
}

func TestGetAndSetFlags(t *testing.T) {
	page := NewPagePtr()
	var expected uint16 = 100
	SetFlags(page, expected)
	got := GetFlags(page)
	assert.Equal(t, expected, got)
}

func TestGetAndSetLowerOffset(t *testing.T) {
	page := NewPagePtr()
	var expected offset = 100
	SetLowerOffset(page, expected)
	got := GetLowerOffset(page)
	assert.Equal(t, expected, got)
}

func TestGetAndSetUpperOffset(t *testing.T) {
	page := NewPagePtr()
	var expected offset = 100
	SetUpperOffset(page, expected)
	got := GetUpperOffset(page)
	assert.Equal(t, expected, got)
}

func TestGetAndSetSpecialSpaceOffset(t *testing.T) {
	page := NewPagePtr()
	var expected offset = 100
	SetSpecialSpaceOffset(page, expected)
	got := GetSpecialSpaceOffset(page)
	assert.Equal(t, expected, got)
}

func TestFlagsBits(t *testing.T) {
	page := NewPagePtr()
	assert.False(t, IsAllVisible(page))

	SetAllVisible(page)
	assert.True(t, IsAllVisible(page))

	ClearAllVisible(page)
	assert.False(t, IsAllVisible(page))
}
