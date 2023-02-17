package page

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

// SlotIndex is the index of the slot within page
// this is not byte offset. the first slot's index is 0 and the next one's index is 1....
type SlotIndex uint16

// see: https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/storage/off.h#L26-L28
const (
	// first slot index
	FirstSlotIndex SlotIndex = 0
	// max slot index
	MaxSlotIndex SlotIndex = PageSize / slotSize
	// invalid slot index
	// maybe this invalid thing should be expressed with error type
	InvalidSlotIndex SlotIndex = MaxSlotIndex + 1
)

// GetSlot returns page slot
func GetSlot(page PagePtr, idx SlotIndex) (SlotPtr, error) {
	// validate slot index
	if idx > MaxSlotIndex {
		return nil, errors.Errorf("invalid slot %d", idx)
	}
	// calculate byte size from slotsOffset
	size := uint16(idx) * slotSize
	so := uint16(slotsOffset) + size
	return SlotPtr(page[so : so+slotSize]), nil
}

// insertSlot adds new slot
// the slot's flag is normal
func insertSlot(page PagePtr, si SlotIndex, offset itemOffset, size itemSize) {
	slot := generateSlot(offset, slotFlagNormal, size)
	so := uint16(slotsOffset) + uint16(si)*slotSize
	binary.LittleEndian.PutUint32(page[so:so+slotSize], uint32(slot))
}

// findFreeSlot finds unused slot
func findFreeSlot(page PagePtr) (SlotIndex, error) {
	nidx := GetNSlotIndex(page)
	// when there is no slot, return invalid
	if nidx == InvalidSlotIndex {
		return InvalidSlotIndex, nil
	}
	for i := FirstSlotIndex; i <= nidx; i++ {
		slot, err := GetSlot(page, i)
		if err != nil {
			return InvalidSlotIndex, errors.Errorf("index is invalid %d", i)
		}
		if IsUnused(slot) {
			return i, nil
		}
	}
	return InvalidSlotIndex, nil
}

// extendSlot extends slot
func extendSlot(page PagePtr) (SlotIndex, error) {
	nidx := GetNSlotIndex(page)
	// when there is no slot, return first slot index
	if nidx == InvalidSlotIndex {
		return FirstSlotIndex, nil
	}
	extendedIdx := nidx + 1
	if extendedIdx > MaxSlotIndex {
		return InvalidSlotIndex, errors.Errorf("slot cannot be extended anymore: %d", extendedIdx)
	}
	return extendedIdx, nil
}

// GetNSlotIndex returns the index of biggest page slot index which has been allocated
// this returns invalid slot index when no slot has been allocated
func GetNSlotIndex(page PagePtr) SlotIndex {
	lo := GetLowerOffset(page)
	si := SlotIndex((lo - slotsOffset) / slotSize)
	if si == 0 {
		// no slot has been allocated
		return InvalidSlotIndex
	}
	return si - 1
}
