package page

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetItem(t *testing.T) {
	page := NewPagePtr()
	InitializePage(page, 10)

	// insert for the first time
	item := []byte{1, 2, 3, 4, 5, 6}
	err := AddItem(page, item, InvalidSlotIndex)
	assert.Nil(t, err)

	got, err := GetItem(page, FirstSlotIndex)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal([]byte(got), item))

	// update the item
	i := ([]byte)(got)
	assert.Equal(t, 1, int(i[0]))
	copy(i[0:1], []byte{5})

	// check whether the memory within the page is updated
	got, err = GetItem(page, FirstSlotIndex)
	assert.Nil(t, err)
	ii := ([]byte)(got)
	assert.Equal(t, 5, int(ii[0]))
}

func TestAddItem(t *testing.T) {
	t.Run("when extend new slot", func(t *testing.T) {
		page := NewPagePtr()
		InitializePage(page, 10)

		// insert for the first time
		item := []byte{1, 2, 3, 4, 5, 6}
		err := AddItem(page, item, InvalidSlotIndex)
		assert.Nil(t, err)

		got, err := GetItem(page, FirstSlotIndex)
		assert.Nil(t, err)
		assert.True(t, bytes.Equal([]byte(got), item))

		// insert second time
		item = []byte{7, 8}
		err = AddItem(page, item, InvalidSlotIndex)
		assert.Nil(t, err)

		got, err = GetItem(page, FirstSlotIndex+1)
		assert.Nil(t, err)
		assert.True(t, bytes.Equal([]byte(got), item))
	})

	t.Run("when there is unused slot", func(t *testing.T) {
		page := NewPagePtr()
		InitializePage(page, 10)

		// insert for the first time
		item := []byte{1, 2, 3, 4, 5, 6}
		err := AddItem(page, item, InvalidSlotIndex)
		assert.Nil(t, err)
		item = []byte{7, 8}
		err = AddItem(page, item, InvalidSlotIndex)
		assert.Nil(t, err)

		// check n slot index
		si := GetNSlotIndex(page)
		assert.Equal(t, FirstSlotIndex+1, si)

		// free up first slot
		slot, err := GetSlot(page, FirstSlotIndex)
		assert.Nil(t, err)
		SetUnused(slot)
		// then add item
		expected := []byte{9, 10}
		err = AddItem(page, expected, InvalidSlotIndex)
		assert.Nil(t, err)

		// check n slot index and confirm slot is not extended
		si = GetNSlotIndex(page)
		assert.Equal(t, FirstSlotIndex+1, si)
		// check the inserted item
		got, err := GetItem(page, FirstSlotIndex)
		assert.Nil(t, err)
		assert.True(t, bytes.Equal([]byte(got), expected))
	})
}
