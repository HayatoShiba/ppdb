package fsm

import "github.com/HayatoShiba/ppdb/storage/page"

// relationPageID is physical page id of relation(table). this is alias of page.PageID
// this is defined to differentiate from fsm's PageID
// the location of the leaf node in the bottom layer of free space map indicates the relation page id
// with relationPageID, we can locate which page has enough free space to insert tuple.
type relationPageID page.PageID

// getAddressFromRelationPageID returns fsm address calculated from relation's page id.
// when update the free space of relation's page, use this function to find the location in fsm.
func getAddressFromRelationPageID(pageID relationPageID) (address, fsmSlot) {
	// page id is the same as the fsm logical page id of bottom level
	fsmPageID := int(pageID) / leafNodeNum
	slot := fsmSlot(int(pageID) % leafNodeNum)
	return address{
		// actual free space of each (relation's) page is stored in bottom layer
		treeLevel:     treeLevelBottom,
		logicalPageID: logicalPageID(fsmPageID),
	}, slot
}

// getRelationPageIDFromAddress returns relation's page id calculated from fsm address and slot
func getRelationPageIDFromAddress(addr address, slot fsmSlot) (relationPageID, bool) {
	// relation's page id is stored in the bottom layer
	if addr.treeLevel != treeLevelBottom {
		return relationPageID(page.InvalidPageID), false
	}
	relPageID := int(addr.logicalPageID)*leafNodeNum + int(slot)
	return relationPageID(relPageID), true
}
