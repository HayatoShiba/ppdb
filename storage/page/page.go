/*
Page is the unit of I/O in ppdb.
Disk manager organizes file as a collection of pages.
Page in ppdb may be called `block“ in postgres.
Page is used not only by the main disk manager but also by the clog/wal disk manager
*/
package page

import (
	"encoding/binary"
	"math"
)

/*
PageSize is the byte size of page. 8KB is the default size in postgres
see block_size parameter in https://www.postgresql.org/docs/current/runtime-config-preset.html

Linux OS page size is probably 4KB so torn page(partial writes) can happen.
This can be avoided by full page writes (the functionality of WAL)
Full page writes is probably so-called `physical logging` (not `logical logging` or `physiological logging`)
see https://github.com/postgres/postgres/blob/5e7bbb528638c0f6d585bab107ec7a19e3a39deb/src/backend/storage/page/README#L36-L46
*/
const PageSize = 8192

// PageID is the unique identifier given to each page, which is called blockNumber in postgres
// see https://github.com/postgres/postgres/blob/d63d957e330c611f7a8c0ed02e4407f40f975026/src/include/storage/block.h#L17-L31
type PageID uint32

const (
	// first page id in file
	FirstPageID PageID = 0
	// invalid page id
	InvalidPageID PageID = math.MaxUint32
	// max page id
	MaxPageID PageID = math.MaxUint32 - 1
)

// PagePtr is pointer to page
// ppdb defines page as pointer explicitly
// because page should not be passed by value in many cases (for concurrent access and space-efficiency)
// (although, using pointer here may be controversial)
type PagePtr *[PageSize]byte

// NewPagePtr returns 0-filled page pointer
func NewPagePtr() PagePtr {
	p := &[PageSize]byte{}
	return PagePtr(p)
}

// InitializePage initializes page
// when extending new page, the page is 0-filled, so should be initialized with this function
// see https://github.com/postgres/postgres/blob/2cd2569c72b8920048e35c31c9be30a6170e1410/src/backend/storage/page/bufpage.c#L35-L42
func InitializePage(p PagePtr, specialSpaceSize uint16) {
	SetLSN(p, 0)
	SetFlags(p, 0)
	SetLowerOffset(p, slotsOffset)
	// upperOffset must be calculated from special space size
	upper := offset(PageSize - specialSpaceSize)
	SetUpperOffset(p, upper)
	SetSpecialSpaceOffset(p, upper)
}

// IsInitialized checks whether the page has been already initialized
// when the upperOffset is 0, then the page isn't initialized
// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/include/storage/bufpage.h#L231
func IsInitialized(p PagePtr) bool {
	lo := binary.LittleEndian.Uint16(p[upperOffsetOffset:specialSpaceOffsetOffset])
	return lo != 0
}

// CalculateFileOffset calculates the page's offset within the file
// the page size is fixed (8KB) so that it is easy to calculate the offset
func CalculateFileOffset(pageID PageID) int64 {
	return int64(pageID * PageSize)
}
