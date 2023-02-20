/*
Visibility map is mainly for the performance improvement of vacuum and index only scan.
With visibility map
- vacuum can decide whether it can skip vacuuming the page
- index only scan can decide whether it has to check the tuple status(which may lead to disk IO).

The vm status of each relation's page is represented with 2bits.

-----
The lower bit

The lower bit indicates whether all tuples in the page is visible to all transactions.
This implies that the tuple hasn't been modified and dead tuples hasn't been generated since
last time vacuum was executed.
If all tuples are visible, then we can skip the vacuum of the page for the reason of no dead tuples.
This leads to avoiding unnecessary scan (disk IO), so improve performance.

flip the bit when:
- when vacuum is executed to the page, flip the bit to `all-visible`
- when transaction updates/deletes tuples in the page, flip the bit to `not all-visible`
  - maybe insert also flip the bit? because new tuple may be dead if the transaction is aborted.

-----
The higher bit

The higher bit indicates whether all tuples on the page has been frozen.
When vacuum scans a page and if all tuples are/become frozen, then flip this bit.
This flipped bit implies that the tuple hasn't been modified since last time vacuum was executed and still all frozen.

----
Interface

- UpdateStatus(page.PagePtr, page.PageID, flags): update the status of pageID with flags
- GetStatus(page.PagePtr, page.PageID): get the status of pageID

----
Others

see https://github.com/postgres/postgres/blob/97c61f70d1b97bdfd20dcb1f2b1be42862ec88c2/src/backend/access/heap/visibilitymap.c#L3
*/
package vm

import "github.com/HayatoShiba/ppdb/storage/page"

// UpdateStatus updates the status of page with the flag
// the caller has to hold pin and exclusive content lock on the buffer which stores the page.
func UpdateStatus(p page.PagePtr, relPageID page.PageID, flags uint8) {
	// calculate the byte offset and the bit offset of vm node from relation's page id
	addr := getAddressFromPageID(relPageID)

	// update the bits we want to update to 00. other bits are not changed
	mask := byte((0x03 << (6 - addr.bitOffset)))
	b := p[addr.byteOffset] & ^mask

	// then use | mask to update the bits
	p[addr.byteOffset] = b | (flags << (6 - addr.bitOffset))
}

// GetStatus gets the status of page
// the caller has to hold pin and shared content lock on the buffer which stores the page.
func GetStatus(p page.PagePtr, relPageID page.PageID) uint8 {
	// calculate the byte offset and the bit offset of vm node from relation's page id
	addr := getAddressFromPageID(relPageID)

	// shift the bits we want to the lowest position
	b := p[addr.byteOffset] >> (6 - addr.bitOffset)
	// then use & mask to get the lowest position
	mask := byte((1 << 2) - 1)
	b = b & mask
	return b
}

const (
	// each page's status is represented with 2 bits
	nodeBits = 2
	// root node is stored at LowerOffset
	rootNodeOffset = page.LowerOffsetOffset
	// the actual page size for visibility map
	pageSize = int(page.PageSize - rootNodeOffset)

	// how many vm node can be stored within a byte
	nodeNumPerByte = 8 / nodeBits
	// how many vm node can be stored within a vm page
	nodeNumPerPage = pageSize / nodeBits
)

// GetVMPageIDFromPageID returns vm page id which contains the node of relation's page id
func GetVMPageIDFromPageID(relPageID page.PageID) page.PageID {
	vmPageID := relPageID / page.PageID(nodeNumPerPage)
	return vmPageID
}

// address is the address of vm bits
// this consists of two fields and it should be sufficient to locate the vm node within the vm page.
type address struct {
	// byteOffset is byte offset within the vm page
	byteOffset uint
	// bitOffset is bit offset within the byte in the vm page. this value can be 0,2,4,6.
	bitOffset uint
}

// getAddressFromPageID returns the byte offset of the vm node from page id
func getAddressFromPageID(relPageID page.PageID) *address {
	byteOffset := (relPageID % page.PageID(nodeNumPerPage)) / nodeNumPerByte
	bitOffset := (relPageID % nodeNumPerByte) * nodeBits
	return &address{
		byteOffset: uint(byteOffset),
		bitOffset:  uint(bitOffset),
	}
}
