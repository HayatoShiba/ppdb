/*
In postgres, page is implemented with the data layout called `slotted page`.
Slotted page has slot array in page header which points to tuple(kind-of row) within the page.
The figure below may be helpful to understand the structure. This is cited from postgres comment.
`linp` in the figure is `slot` and
the space from `pd_lower` to `pd_upper` is free space where the tuple will be inserted later.

  - +----------------+---------------------------------+
  - | PageHeaderData | linp1 linp2 linp3 ...           |
  - +-----------+----+---------------------------------+
  - | ... linpN |									  |
  - +-----------+--------------------------------------+
  - |		   ^ pd_lower							  |
  - |												  |
  - |			 v pd_upper							  |
  - +-------------+------------------------------------+
  - |			 | tupleN ...                         |
  - +-------------+------------------+-----------------+
  - |	   ... tuple3 tuple2 tuple1 | "special space" |
  - +--------------------------------+-----------------+

see: https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/include/storage/bufpage.h#L29-L42

The structure of slotted page is mainly for the table's tuple,
But this structure is also used with fsm/vm/clog/wal although slot is not used.
`slot` within the structure is beneficial for mainly two reasons
- the variable size of tuple can be stored
  - without slot, the offset of each tuple whose size is variable cannot be identified easily
    while the offset can be calculated easily without slot when the size of tuple is fixed

- tuple can be moved/removed if necessary
  - for example,
    index points to slot, not the actual location of tuple.
    when the tuple is moved to other location within the page, index doesn't have to be updated.
    just update the slot and make the slot point to the new location of tuple.
    this happens when vacuum removes dead tuples and compact the page
*/
package page

import (
	"encoding/binary"

	"github.com/HayatoShiba/ppdb/common"
)

// header is header of slotted page and defined just for understanding page header structure
// actually this struct is not used anywhere because parsing the whole page header seems to be inefficient (I think)
// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/include/storage/bufpage.h#L109-L155
// see also https://www.postgresql.org/docs/current/storage-page-layout.html
type header struct {
	// in ppdb, pd_checksum is omitted for simplicity

	// this is called pd_lsn in postgres
	// lsn is log sequence number and this is used for confirming shared buffer pool manager policy
	// the policy in postgres is steal/no-force and, for more details, see /storage/buffer/manager.go
	// maybe this lsn is not used in ppdb
	lsn common.WALRecordPtr

	// this is called pd_flags in postgres
	// flags stores page information
	flags uint16

	// this is called pd_lower in postgres
	// lowerOffset ~ upperOffset is free space
	// this space is used for the insertion of new tuples
	lowerOffset offset
	// this is called pd_upper in postgres
	upperOffset offset

	// this is called pd_special in postgres
	// special space can contain anything the access method wishes to store. index leaf node right/left sibling page id can be stored.
	specialSpaceOffset offset

	// pd_prune_xid is omitted. define it later if necessary
}

// offset is the byte offset within the page
type offset uint16

// byte offset of page header
const (
	// lsn is defined at the head of page
	lsnOffset offset = 0
	// lsn is defined as uint64, so add 8 bytes
	flagsOffset offset = lsnOffset + 8
	// flags is defined as uint16, so add 2 bytes
	lowerOffsetOffset offset = flagsOffset + 2
	// lowerOffset is defined as uint16, so add 2 bytes
	upperOffsetOffset offset = lowerOffsetOffset + 2
	// upperOffset is defined as uint16, so add 2 bytes
	specialSpaceOffsetOffset offset = upperOffsetOffset + 2
	// specialSpaceOffset is defined as uint16, so add 2 bytes
	slotsOffset offset = specialSpaceOffsetOffset + 2
)

// GetLSN returns lsn
func GetLSN(p PagePtr) common.WALRecordPtr {
	lsn := binary.LittleEndian.Uint64(p[lsnOffset:flagsOffset])
	return common.WALRecordPtr(lsn)
}

// SetLSN sets lsn
func SetLSN(p PagePtr, lsn common.WALRecordPtr) {
	binary.LittleEndian.PutUint64(p[lsnOffset:flagsOffset], uint64(lsn))
}

// GetFlags returns flags
func GetFlags(p PagePtr) uint16 {
	return binary.LittleEndian.Uint16(p[flagsOffset:lowerOffsetOffset])
}

// SetFlags sets flags
func SetFlags(p PagePtr, flags uint16) {
	binary.LittleEndian.PutUint16(p[flagsOffset:lowerOffsetOffset], flags)
}

// GetLowerOffset returns lower offset
func GetLowerOffset(p PagePtr) offset {
	loc := binary.LittleEndian.Uint16(p[lowerOffsetOffset:upperOffsetOffset])
	return offset(loc)
}

// SetLowerOffset sets lower offset
func SetLowerOffset(p PagePtr, o offset) {
	binary.LittleEndian.PutUint16(p[lowerOffsetOffset:upperOffsetOffset], uint16(o))
}

// GetUpperOffset returns upper offset
func GetUpperOffset(p PagePtr) offset {
	loc := binary.LittleEndian.Uint16(p[upperOffsetOffset:specialSpaceOffsetOffset])
	return offset(loc)
}

// SetUpperOffset sets upper offset
func SetUpperOffset(p PagePtr, o offset) {
	binary.LittleEndian.PutUint16(p[upperOffsetOffset:specialSpaceOffsetOffset], uint16(o))
}

// GetSpecialSpaceOffset returns special space offset
func GetSpecialSpaceOffset(p PagePtr) offset {
	loc := binary.LittleEndian.Uint16(p[specialSpaceOffsetOffset:slotsOffset])
	return offset(loc)
}

// SetSpecialSpaceOffset sets special space offset
func SetSpecialSpaceOffset(p PagePtr, o offset) {
	binary.LittleEndian.PutUint16(p[specialSpaceOffsetOffset:slotsOffset], uint16(o))
}

// flags utility functions
// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/include/storage/bufpage.h#L172-L186
const (
	allVisible = 0x01
)

// IsAllVisible is whether the flags allVisible is set
func IsAllVisible(p PagePtr) bool {
	flags := GetFlags(p)
	return (flags & allVisible) != 0
}

// SetAllVisible sets allVisible bit
func SetAllVisible(p PagePtr) {
	flags := GetFlags(p)
	SetFlags(p, flags|allVisible)
}

// ClearAllVisible clears allVisible bit
func ClearAllVisible(p PagePtr) {
	flags := GetFlags(p)
	SetFlags(p, flags&^allVisible)
}
