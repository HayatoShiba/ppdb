package page

import (
	"encoding/binary"
)

// SlotPtr is pointer to slot WITHIN PAGE
// basically this type is used for slot
type SlotPtr *[slotSize]byte

// slotSize is the byte size of Slot. Slot is defined with uint32
const slotSize = 4

/*
Slot is used for calculation or bit operation of slot
basically, SlotPtr type is used instead of Slot

Slot consists of three fields
- item offset/uint15. this is the offset of the item which slot points to
- flag/uint2. flag indicates whether this slot is normal/unused/HOT redirected/dead
- item size/uint15. this is the byte size of the item. this field is necessary because the item's length can be variable
see: https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/storage/itemid.h#L17-L30
*/
type Slot uint32

// slotFlag is flag stored in Slot
// slotFlag is used when extracting flag from Slot and assigning it to variable
// see https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/storage/itemid.h#L34-L41
type slotFlag uint8

// see https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/storage/itemid.h#L34-L41
const (
	// slotFlagUnused indicates the slot was freed by vacuum
	slotFlagUnused slotFlag = iota
	// slotFlagNormal indicates the slot is currently used
	slotFlagNormal
	// slotFlagRedirect indicates the slot is HOT redirected (probably this is not used in ppdb)
	slotFlagRedirected
	// slotFlagDead indicates the slot is dead (this is used by vacuum process)
	slotFlagDead
)

// generateSlot generates slot from offset 15bit, flags 2bit, size 15bit
func generateSlot(io itemOffset, flag slotFlag, size itemSize) Slot {
	var slot uint32
	slot |= (uint32(io) << 17)
	slot |= (uint32(flag) << 15)
	slot |= (uint32(size))
	return Slot(slot)
}

// getItemOffset returns item offset
func getItemOffset(s SlotPtr) itemOffset {
	slot := convertSlot(s)
	return itemOffset(uint32(slot) >> 17)
}

// setItemOffset updates item offset
func setItemOffset(s SlotPtr, io itemOffset) {
	slot := convertSlot(s)
	// reset item offset
	var mask uint32 = (1 << 17) - 1
	tmp := uint32(slot) & mask
	// insert new item offset
	var newio uint32 = uint32(io) << 17
	newSlot := tmp + newio
	binary.LittleEndian.PutUint32(s[:], newSlot)
}

// getItemSize returns item size
func getItemSize(s SlotPtr) itemSize {
	slot := convertSlot(s)
	mask := uint32((1 << 15) - 1)
	return itemSize(uint32(slot) & mask)
}

// getItemSize returns item size
func getFlag(s SlotPtr) slotFlag {
	slot := convertSlot(s)
	mask := uint32((1 << 15) | (1 << 16))
	flag := (uint32(slot) & mask) >> 15
	return slotFlag(flag)
}

// IsUnused checks whether the page slot is used
func IsUnused(s SlotPtr) bool {
	return getFlag(s) == slotFlagUnused
}

// SetUnused sets flag to unused
// this is expected to be used mainly when vacuum frees up tuple
func SetUnused(s SlotPtr) {
	slot := convertSlot(s)
	var mask uint32 = (1 << 15) | (1 << 16)
	newSlot := uint32(slot) & ^mask
	binary.LittleEndian.PutUint32(s[:], newSlot)
}

// IsNormal checks whether the page slot is normal
func IsNormal(s SlotPtr) bool {
	return getFlag(s) == slotFlagNormal
}

// SetNormal sets flag to normal
// this is expected to be used mainly when unused slot is re-used
func SetNormal(s SlotPtr) {
	slot := convertSlot(s)

	var mask1 uint32 = 1 << 15
	newSlot := uint32(slot) | mask1
	var mask2 uint32 = (1 << 16)
	newSlot = newSlot & (^mask2)

	binary.LittleEndian.PutUint32(s[:], newSlot)
}

// IsRedirected checks whether the page slot is redirected
func IsRedirected(s SlotPtr) bool {
	return getFlag(s) == slotFlagRedirected
}

// SetRedirected sets flag to redirected
func SetRedirected(s SlotPtr) {
	slot := convertSlot(s)

	var mask1 uint32 = 1 << 16
	newSlot := uint32(slot) | mask1
	var mask2 uint32 = (1 << 15)
	newSlot = newSlot & (^mask2)

	binary.LittleEndian.PutUint32(s[:], newSlot)
}

// IsDead checks whether the page slot is dead
func IsDead(s SlotPtr) bool {
	return getFlag(s) == slotFlagDead
}

// SetDead sets flag to dead
func SetDead(s SlotPtr) {
	slot := convertSlot(s)

	var mask uint32 = (1 << 15) | (1 << 16)
	newSlot := uint32(slot) | mask

	binary.LittleEndian.PutUint32(s[:], newSlot)
}

// convertSlot converts slot pointer to slot for calculation or bit operation
func convertSlot(s SlotPtr) Slot {
	return Slot(binary.LittleEndian.Uint32(s[:]))
}
