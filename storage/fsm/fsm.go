/*
the implementation of free space map structure
*/
package fsm

import "github.com/HayatoShiba/ppdb/storage/page"

const (
	// root node is stored at lowerOffsetOffset of page
	rootNodeOffset = page.LowerOffsetOffset

	// the number of fsm node within page
	// (the node size is 1 byte)
	nodeNum = int(page.PageSize - rootNodeOffset)

	// the number of leaf node within page
	// the calculation is from the nature of binary tree
	// important: the binary tree is not perfect, so the number of leaf node is not nodeNum/2
	// (the node size is 1 byte)
	nonLeafNodeNum = nodeNum/2 - 1
	// the binary tree within page is not perfect because of page header
	// so calculate the number of leaf node like below
	leafNodeNum = nodeNum - nonLeafNodeNum
)

// address is the address of the free space of each page
type address struct {
	treeLevel     treeLevel
	logicalPageID logicalPageID
}

// treeLevel is tree level of fsm binary tree over page
type treeLevel uint

const (
	// it is important for the tree depth to be capable of storing all pages in one relation(table)
	// PageID is defined with uint32 so the maximum id of page is 2^32 and this can be stored when tree level is 3
	// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/backend/storage/freespace/freespace.c#L68-L78
	treeLevelDepth            = 3
	treeLevelRoot   treeLevel = treeLevelDepth - 1
	treeLevelBottom treeLevel = 0
)

// logicalPageID is fsm logical page id
// logical page id is allocated per each tree level
type logicalPageID uint

const (
	firstLogicalPageID logicalPageID = 0
)

// fsmSlot is the fsm slot within the page
type fsmSlot int

const (
	// invalid fsm slot
	invalidSlot fsmSlot = -1
	// first fsm slot
	firstSlot fsmSlot = 0
)

// fsmPageID is physical page id of fsm, not relation. this is alias of PageID.
// this is defined to differentiate from relation's PageID
type fsmPageID page.PageID

// fsmRootPageID is the top page of root level. this is kind of entry point.
const fsmRootPageID fsmPageID = 0

// getFSMPageIDFromAddress gets fsm page id from address
// this calculation is a bit complicated
// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/backend/storage/freespace/freespace.c#L432
func getFSMPageIDFromAddress(addr address) fsmPageID {
	leafNumInBottomLevel := uint(addr.logicalPageID)
	// ex: if root level and logical page id is 1, then
	// which means that, in middle level, leafNodeNum exists, and bottom level leafNodeNum exists
	for i := addr.treeLevel; i > treeLevelBottom; i-- {
		leafNumInBottomLevel *= uint(leafNodeNum)
	}
	var pid fsmPageID
	// add page id from bottom level by level
	for level := 0; level < treeLevelDepth; level++ {
		// because of the offset, +1
		pid += fsmPageID(leafNumInBottomLevel + 1)
		// the next level nodes
		leafNumInBottomLevel /= uint(leafNodeNum)
	}
	pid -= fsmPageID(addr.treeLevel)
	return pid - 1
}
