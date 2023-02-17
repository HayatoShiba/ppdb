/*
`item` is interchangeably used with `heap tuple` / `index tuple` ...

item-related interface is
- GetItem(PagePtr, SlotIndex): gets item from page. the location of the item is calculated from SlotIndex's Slot
- AddItem(PagePtr, ItemPtr, SlotIndex): adds item to the page. if the page does not have enough space, return error.
*/
package page

import (
	"github.com/pkg/errors"
)

// ItemPtr points to item within page
// item length is variable
type ItemPtr []byte

// itemOffset is the byte offset of the item within page
// itemOffset is used when extracting item offset from Slot and assigning it to variable
// see Slot type for more details
type itemOffset uint16

// itemSize is the size of the item
// itemSize is used when extracting item size from Slot and assigning it to variable
// see Slot type for more details
type itemSize uint16

// GetItem returns the item pointed by the slot
func GetItem(page PagePtr, idx SlotIndex) (ItemPtr, error) {
	// get slot from slot's index
	slot, err := GetSlot(page, idx)
	if err != nil {
		return nil, errors.Wrap(err, "GetSlot failed")
	}
	io := getItemOffset(slot)
	size := getItemSize(slot)
	ptr := page[io : uint16(io)+uint16(size)]
	return ItemPtr(ptr), nil
}

/*
AddItem adds item to the page
AddItem does the following
- get slot index where the item will be inserted: find free slot or, when no free slot, extend new slot
- generate slot data and insert it to the slot index.
- insert item to the page
- update page header
see https://github.com/postgres/postgres/blob/2cd2569c72b8920048e35c31c9be30a6170e1410/src/backend/storage/page/bufpage.c#L194
*/
func AddItem(page PagePtr, item ItemPtr, si SlotIndex) error {
	var slotExtended bool
	var err error
	slotIndex := si
	// if invalid slot index is passed, find free slot or extend new slot
	if slotIndex == InvalidSlotIndex {
		slotIndex, err = findFreeSlot(page)
		if err != nil {
			return errors.Wrap(err, "findFreeSlot failed")
		}
		// if no free slot, just extend the slot
		if slotIndex == InvalidSlotIndex {
			// extend new slot
			slotIndex, err = extendSlot(page)
			if err != nil {
				return errors.Wrap(err, "extendSlot failed")
			}
			slotExtended = true
		}
	}
	// here, the slot is decided, so check space
	size := len(item)
	if ok := hasEnoughFreeSpace(page, size, slotExtended); !ok {
		return errors.Errorf("item size is larger than the free sapce. itemSize %d", size)
	}

	newUpperOffset := uint16(GetUpperOffset(page)) - uint16(size)
	// the item offset which will be inserted is the same as new upper offset
	insertedItemOffset := newUpperOffset
	// add slot
	insertSlot(page, slotIndex, itemOffset(insertedItemOffset), itemSize(size))
	// insert item to the item offset
	copy(page[insertedItemOffset:insertedItemOffset+uint16(size)], item)
	// update page header
	if slotExtended {
		newLowerOffset := GetLowerOffset(page) + slotSize
		SetLowerOffset(page, newLowerOffset)
	}
	SetUpperOffset(page, offset(newUpperOffset))
	return nil
}

// hasEnoughFreeSpace checks whether the page has enough free space to add the item
// if slotExtended is true, then the slot will be extended to add the item, so consider the added slot size
func hasEnoughFreeSpace(page PagePtr, itemSize int, slotExtended bool) bool {
	freeSpace := CalculateFreeSpace(page)
	// if slot will be extended, the slot size should be considered
	if slotExtended {
		itemSize = itemSize + slotSize
	}
	return freeSpace >= int(itemSize)
}
