// page tree is tree structures of pages.
// the functions about tree structures WITHIN page is defined tree.go.
package fsm

// getChildAddress ges child fsm page's fsm address calculated from parent fsm address/slot.
// this function is expected to be called to go down to the next tree level
func getChildAddress(addr address, slot fsmSlot) (address, bool) {
	// tree level bottom does not have any child
	if addr.treeLevel == treeLevelBottom {
		return address{}, false
	}
	logPageID := int(addr.logicalPageID)*leafNodeNum + int(slot)
	return address{
		treeLevel:     addr.treeLevel - 1,
		logicalPageID: logicalPageID(logPageID),
	}, true
}

// getParentAddress ges parent fsm page's fsm address/slot calculated from child fsm address.
// this function is expected to be called to go up to the next tree level
func getParentAddress(addr address) (address, fsmSlot, bool) {
	// tree root level does not have any parent
	if addr.treeLevel == treeLevelRoot {
		return address{}, invalidSlot, false
	}
	logPageID := int(addr.logicalPageID) / (leafNodeNum + 1)
	slot := int(addr.logicalPageID) % (leafNodeNum + 1)
	return address{
		treeLevel:     addr.treeLevel + 1,
		logicalPageID: logicalPageID(logPageID),
	}, fsmSlot(slot), true
}
