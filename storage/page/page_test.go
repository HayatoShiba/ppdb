package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPageInitialized(t *testing.T) {
	tests := []struct {
		name        string
		initialized bool
		expected    bool
	}{
		{
			name:        "page has not been initialized",
			initialized: false,
			expected:    false,
		},
		{
			name:        "page has been initialized",
			initialized: true,
			expected:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := NewPagePtr()
			if tt.initialized {
				InitializePage(page, 10)
			}
			got := IsInitialized(page)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestCalculateFreeSpace(t *testing.T) {
	page := NewPagePtr()
	InitializePage(page, 10)

	got := CalculateFreeSpace(page)
	expected := PageSize - slotsOffset - 10
	assert.Equal(t, int(expected), got)

	var item []byte = []byte{1, 2}
	err := AddItem(page, ItemPtr(item), InvalidSlotIndex)
	assert.Nil(t, err)

	got = CalculateFreeSpace(page)
	size := int(expected) - slotSize - len(item)
	assert.Equal(t, size, got)
}

func TestCompactPage(t *testing.T) {
	page := NewPagePtr()
	InitializePage(page, 10)

	got := CalculateFreeSpace(page)
	expected := PageSize - slotsOffset - 10
	assert.Equal(t, int(expected), got)

	// insert item
	num := 5
	for i := 0; i < num; i++ {
		item := []byte{1, 2, 3, 4, 5, 6}
		err := AddItem(page, item, InvalidSlotIndex)
		assert.Nil(t, err)
	}

	// calculate free space size
	got = CalculateFreeSpace(page)
	expected = expected - (slotSize+6)*5
	assert.Equal(t, int(expected), got)

	// free up second slot
	slot, err := GetSlot(page, FirstSlotIndex+1)
	assert.Nil(t, err)
	SetUnused(slot)

	// compact page
	err = CompactPage(page)
	assert.Nil(t, err)

	// re-calculate free space size
	got = CalculateFreeSpace(page)
	// 6 byte is the size of item freed up
	expected = expected + 6
	assert.Equal(t, int(expected), got)

	// insert one more item
	item := []byte{1, 2, 3, 4, 5, 6}
	err = AddItem(page, item, InvalidSlotIndex)
	assert.Nil(t, err)

	// free up fourth slot
	slot, err = GetSlot(page, FirstSlotIndex+3)
	assert.Nil(t, err)
	SetUnused(slot)

	// compact page one more time
	err = CompactPage(page)
	assert.Nil(t, err)

	// re-calculate free space size
	got = CalculateFreeSpace(page)
	// add one item and frees up one item, so the free space must not be changed
	assert.Equal(t, int(expected), got)
}
