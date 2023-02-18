// tree is tree structures within pages.
// the functions about tree structures OF page is defined page_tree.go.
package fsm

// nodeIndex is the index of fsm binary tree node. this is not byte offset within page.
// if index is 0, the node is root node
// if index is 1, the node is left child of root node
// fsm node size is 1 byte (see space.go)
type nodeIndex uint

const (
	rootNodeIndex nodeIndex = 0
)

// getLeftChildNode returns the index of left child node of binary tree within page.
// this is expected to be called to go down the binary tree within fsm page.
func getLeftChildNode(index nodeIndex) nodeIndex {
	return index*2 + 1
}

// getRightChildNode returns the index of right child node of binary tree within page.
// this is expected to be called to go down the binary tree within fsm page.
func getRightChildNode(index nodeIndex) nodeIndex {
	return index*2 + 2
}

// getParentNode returns the index of parent node of binary tree within page.
// this is expected to be called to go up the binary tree within fsm page.
func getParentNode(index nodeIndex) nodeIndex {
	return (index - 1) / 2
}

// getSlotFromNodeIndex returns the slot within page calculated from fsm node index
// when binary search reaches the leaf node, the node index has to be converted into fsm slot for searching the next child tree on another fsm page
func getSlotFromNodeIndex(index nodeIndex) (fsmSlot, bool) {
	slot := int(index) - nonLeafNodeNum
	// if index is smaller than nonLeafNodeNum, then there is no slot.
	if slot <= 0 {
		return 0, false
	}
	return fsmSlot(slot - 1), true
}

// isLeaf checks whether the node is leaf node or not
func isLeaf(index nodeIndex) bool {
	slot := int(index) - nonLeafNodeNum
	if slot <= 0 {
		return false
	}
	return true
}

// isRoot checks whether the node is root node or not
func isRoot(index nodeIndex) bool {
	return index == rootNodeIndex
}

// getByteOffsetFromNodeIndex returns the byte offset of the node within the page
// the size of each node is 1 byte, so just add the index to get the byte offset of the node
// this is expected to be called when get/update the free space size within the node.
func getByteOffsetFromNodeIndex(index nodeIndex) uint16 {
	return rootNodeOffset + uint16(index)
}

// getNodeIndexFromSlot returns the node index within page calculated from fsm slot
func getNodeIndexFromSlot(slot fsmSlot) nodeIndex {
	return nodeIndex(int(slot) + 1 + nonLeafNodeNum)
}
