package tuple

import (
	"encoding/binary"

	"github.com/HayatoShiba/ppdb/storage/page"
)

// Tid consists of pageID and slot index
// so, with tid, the tuple can  be located
type Tid struct {
	pageID page.PageID
	slot   page.SlotIndex
}

func NewTid(pid page.PageID, slotIndex page.SlotIndex) Tid {
	return Tid{
		pageID: pid,
		slot:   slotIndex,
	}
}

const (
	// tid size is 8byte (page id is 4byte, slot index is 4byte)
	tidSize = 8
)

// PageID returns page id
func (t *Tid) PageID() page.PageID {
	return t.pageID
}

// SlotIndex returns slot index
func (t *Tid) SlotIndex() page.SlotIndex {
	return t.slot
}

// marshalTid marshals tid
func marshalTid(t Tid) uint64 {
	b := make([]byte, 0, tidSize)
	b = binary.LittleEndian.AppendUint32(b, uint32(t.pageID))
	b = binary.LittleEndian.AppendUint32(b, uint32(t.slot))
	return binary.LittleEndian.Uint64(b)
}

// unmarshalTid unmarshals tid
func unmarshalTid(b []byte) Tid {
	pageID := binary.LittleEndian.Uint32(b[0:4])
	slot := binary.LittleEndian.Uint32(b[4:8])
	return Tid{
		pageID: page.PageID(pageID),
		slot:   page.SlotIndex(slot),
	}
}
