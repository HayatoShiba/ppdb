package page

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateSlot(t *testing.T) {
	tests := []struct {
		name     string
		offset   itemOffset
		flags    slotFlag
		size     itemSize
		expected uint32
	}{
		{
			name:   "flag is unused",
			offset: 1,
			flags:  slotFlagUnused,
			size:   1,
			// "00000000000000100000000000000001"
			expected: 0x20001,
		},
		{
			name:   "flag is normal",
			offset: 1,
			flags:  slotFlagNormal,
			size:   1,
			// "00000000000000101000000000000001"
			expected: 0x28001,
		},
		{
			name:   "flag is redirected",
			offset: 1,
			flags:  slotFlagRedirected,
			size:   1,
			// "00000000000000110000000000000001"
			expected: 0x30001,
		},
		{
			name:   "flag is dead",
			offset: 1,
			flags:  slotFlagDead,
			size:   1,
			// "00000000000000111000000000000001"
			expected: 0x38001,
		},
		{
			name:   "offset and size",
			offset: 10,
			flags:  slotFlagNormal,
			size:   20,
			// "00000000000101001000000000010100"
			expected: 0x148014,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateSlot(tt.offset, tt.flags, tt.size)
			assert.Equal(t, tt.expected, uint32(got))
		})
	}
}

func TestItemOffset(t *testing.T) {
	var expected itemOffset = 10
	s := generateSlot(expected, slotFlagNormal, 20)
	slot := convertSlotPtr(s)
	got := getItemOffset(slot)
	assert.Equal(t, expected, got)

	expected = 100
	setItemOffset(slot, expected)
	got = getItemOffset(slot)
	assert.Equal(t, expected, got)
}

func TestGetItemSize(t *testing.T) {
	var expected itemSize = 10
	s := generateSlot(20, slotFlagNormal, expected)
	slot := convertSlotPtr(s)
	got := getItemSize(slot)
	assert.Equal(t, expected, got)
}

func TestSlotUnused(t *testing.T) {
	s := generateSlot(20, slotFlagDead, 10)
	slot := convertSlotPtr(s)
	assert.False(t, IsUnused(slot))

	SetUnused(slot)
	assert.True(t, IsUnused(slot))
}

func TestSlotNormal(t *testing.T) {
	s := generateSlot(20, slotFlagDead, 10)
	slot := convertSlotPtr(s)
	assert.False(t, IsNormal(slot))

	SetNormal(slot)
	assert.True(t, IsNormal(slot))
}

func TestSlotRedirected(t *testing.T) {
	s := generateSlot(20, slotFlagDead, 10)
	slot := convertSlotPtr(s)
	assert.False(t, IsRedirected(slot))

	SetRedirected(slot)
	assert.True(t, IsRedirected(slot))
}

func TestSlotDead(t *testing.T) {
	s := generateSlot(20, slotFlagUnused, 10)
	slot := convertSlotPtr(s)
	assert.False(t, IsDead(slot))

	SetDead(slot)
	assert.True(t, IsDead(slot))
}

func convertSlotPtr(s Slot) SlotPtr {
	var slot = [4]byte{}
	sp := SlotPtr(&slot)
	binary.LittleEndian.PutUint32(sp[:], uint32(s))
	return sp
}
