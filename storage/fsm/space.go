package fsm

import (
	"github.com/HayatoShiba/ppdb/storage/page"
)

/*
Free space size is defined with 1 byte for space efficiency.
Free space size within page can be 8192 at most because of pase size.
so the mapping is below. (this mapping is cited from postgres)

* Range	 Category
* 0	   - 31   0
* 32   - 63   1
* ...    ...  ...
* 8096 - 8127 253
* 8128 - 8163 254
* 8164 - 8192 255

for more details, see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/backend/storage/freespace/freespace.c#L36-L63
*/
type freeSpaceSize uint8

// convertToFreeSpaceSize converts size to free space size
func convertToFreeSpaceSize(size int) (freeSpaceSize, bool) {
	if (size > page.PageSize) || (size < 0) {
		return 0, false
	}
	if size == 8192 {
		return freeSpaceSize(255), true
	}
	fsSize := size / 32
	return freeSpaceSize(fsSize), true
}

// getFreeSpaceSizeFromNodeIndex returns free space size stored in the node
func getFreeSpaceSizeFromNodeIndex(p page.PagePtr, index nodeIndex) freeSpaceSize {
	offset := getByteOffsetFromNodeIndex(index)
	size := p[offset]
	return freeSpaceSize(size)
}

// updateFreeSpaceSizeFromIndex updates free space size stored in the node
func updateFreeSpaceSizeFromNodeIndex(p page.PagePtr, index nodeIndex, size freeSpaceSize) {
	offset := getByteOffsetFromNodeIndex(index)
	p[offset] = byte(size)
}
