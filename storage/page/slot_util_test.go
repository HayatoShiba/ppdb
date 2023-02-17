package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSlot(t *testing.T) {
	page := NewPagePtr()
	InitializePage(page, 10)
	// add slot
	var si SlotIndex = 1
	insertSlot(page, si, itemOffset(10), itemSize(20))
	got, err := GetSlot(page, si)
	assert.Nil(t, err)
	// "00000000000101001000000000010100"
	var expected uint32 = 0x148014
	assert.Equal(t, expected, uint32(convertSlot(got)))
}
