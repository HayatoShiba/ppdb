/*
Free space map stores the information about free space within each page.
When inserting new tuples, free space map has to be used to find the appropriate page fast to store the tuple.
Free space map is called `page directory` in CMU database lecture.

----
About free space map:

Free space map is generated per relation(table).
Each free space is expressed with ONE BYTE for making the size of fsm small. it leads to faster search.
FSM is not WAL-logged: see https://github.com/postgres/postgres/blob/7db0cde6b58eef2ba0c70437324cbc7622230320/src/backend/storage/freespace/README#L168-L189

free space map uses page layout like the relation(table)

---

how to search enough free space when inserting new tuple:

 1. fetch fsm root page into buffer and pin/lock it
 2. check the root node in the page.
    if the free space within the root node is smaller than we want, then enough free space doesn't exist. extend the file.
 3. if the free space within the root node is bigger than we want, then there is enough free space somewhere.
 4. go down the binary tree within the root page until it reaches the leaf node.
 5. leaf node shows if it is necessary to go down another fsm page for tree. unpin/unlock fsm root page, and
    - fetch fsm child page into buffer and pin/lock it
    - IMPORTANT: maybe another goroutine has updated the free space size.
    - so, when entering this page and finding there is no enough free space unexpectedly(although parent page shows enough free space),
    re-start from fsm root page.
 6. when reaches the bottom tree level and can find slot, then return the page id.

----

The interface for free space map:
- SearchPageWithFSS(): use free space map for locating the enough free space
for the insertion of tuple into page.
  - to find the appropriate page, it has to go down the binary tree until
    it reaches the slot which shows page id

- UpdateFSM(): update free space map when vacuum prune dead tuple and compact the page(de-fragmentation within the page)
  - to update free space map, it has to bubble up the change to upper node/page in binary tree.
  - in postgres, maybe there are other conditions that update free space map in addition to vacuum

----

note: It may be not appropriate to define manager for the operation of free space map because
Free space map consists of binary format + the operation about the structure.
Free space map is related with buffer/file(shared-resource) on disk, but
buffer manager/disk manager are responsible for them respectively. so free space map manager doesn't manage any shared resource.
But ppdb defines free space map manager, because the operation of fsm affects multiple pages for binary tree
and it may be not easy to define free space map operation without buffer manager.

-----

see https://github.com/postgres/postgres/blob/7db0cde6b58eef2ba0c70437324cbc7622230320/src/backend/storage/freespace/README#L1
*/
package fsm

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/buffer"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/pkg/errors"
)

type Manager struct {
	bm *buffer.Manager
}

// NewManager initializes manager
func NewManager(bm *buffer.Manager) *Manager {
	return &Manager{
		bm: bm,
	}
}

/*
SearchPageIDWithFreeSpaceSize searches page which has enough free space size.
this function does
- fetch the root fsm page
- check the root node of root fsm page
- if root node shows that there is relation's page which has enough free space, then
- goes the fsm tree dow to the slot of bottom tree level which indicates the relation's page id we want

when fsm shows no enough free space, then this function returns InvalidPageID.
there is some optimization in postgres, but ppdb does not implement it
see for more details of the optimization:
https://github.com/postgres/postgres/blob/7db0cde6b58eef2ba0c70437324cbc7622230320/src/backend/storage/freespace/README#L80-L87
see also https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/backend/storage/freespace/freespace.c#L702
*/
func (m *Manager) SearchPageIDWithFreeSpaceSize(rel common.Relation, size int) (page.PageID, error) {
	wanted, ok := convertToFreeSpaceSize(size)
	if !ok {
		return page.InvalidPageID, errors.Errorf("the size passed is unexpected: %d", size)
	}

	// fetch root page into buffer.
	// the buffer is pinned and shared content lock is held
	// so the content cannot be evicted, and updated by other goroutines
	exclusive := false
	bufID, err := m.bm.ReadBufferFSM(rel, page.PageID(fsmRootPageID), exclusive)
	if err != nil {
		return page.InvalidPageID, errors.Wrap(err, "ReadBufferFSM failed 1")
	}
	p := m.bm.GetPage(bufID)

	idx := rootNodeIndex
	// at first, check the free space of root node of root fsm page.
	// when it shows no enough free space, return invalid page id
	if getFreeSpaceSizeFromNodeIndex(p, idx) < wanted {
		return page.InvalidPageID, nil
	}

	// when root node shows enough free space, then go down the tree till it reaches the slot of bottom tree level.
	addr := address{
		treeLevel:     treeLevelRoot,
		logicalPageID: firstLogicalPageID,
	}
	for {
		// at first, go down right
		rightIndex := getRightChildNode(idx)
		leftIndex := getLeftChildNode(idx)
		if getFreeSpaceSizeFromNodeIndex(p, rightIndex) >= wanted {
			// when there is enough space at right node, then decide to go down there.
			idx = rightIndex
		} else if getFreeSpaceSizeFromNodeIndex(p, leftIndex) >= wanted {
			idx = leftIndex
		} else {
			return page.InvalidPageID, errors.New("this cannot happen (probably)")
		}

		/*
			patterns:
			- if the right/left child node is leaf node
			  - and tree level is bottom, then calculate relation's page id and return it
			  - and tree level isn't bottom, go down the tree level one more then fetch the fsm page
			- if the right/left child node isn't leaf node
			  - then continue to go down the tree within page
		*/
		if !isLeaf(idx) {
			// if right/left child is not leaf node, then continue to go down the tree
			continue
		}

		slot, _ := getSlotFromNodeIndex(idx)
		// check current tree level
		if addr.treeLevel == treeLevelBottom {
			// calculate relation's page id from address and slot, then return it!
			pageID, ok := getRelationPageIDFromAddress(addr, slot)
			if !ok {
				return page.InvalidPageID, errors.Errorf(
					"getRelationPageIDFromAddress is unexpected: addr %v, slot %v", addr, slot)
			}
			// release buffer
			m.bm.ReleaseBufferFSM(bufID, exclusive)
			return page.PageID(pageID), nil
		}
		// when current tree level is not bottom, then release buffer and fetch next fsm page into buffer and continue
		// unpin/unlock buffer
		m.bm.ReleaseBufferFSM(bufID, exclusive)

		// go down tree level
		childAddr, ok := getChildAddress(addr, slot)
		if !ok {
			return page.InvalidPageID, errors.Errorf("getChildAddress is unexpected: addr %v, slot %v", addr, slot)
		}
		addr = childAddr
		// reset index to search
		idx = rootNodeIndex
		fsmPageID := getFSMPageIDFromAddress(addr)

		// fetch new fsm page into buffer
		bufID, err = m.bm.ReadBufferFSM(rel, page.PageID(fsmPageID), exclusive)
		if err != nil {
			return page.InvalidPageID, errors.Wrap(err, "ReadBufferFSM failed 2")
		}
		p = m.bm.GetPage(bufID)

		// IMPORTANT: at first, check the free space of root node of the page.
		// `there is no enough free space` can happen although we confirmed parent page indicated enough free space.
		// this happens when other goroutine updates the page before the content lock is acquired.
		// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/backend/storage/freespace/freespace.c#L754-L765
		if getFreeSpaceSizeFromNodeIndex(p, idx) < wanted {
			// when this happens, postgres updates the parent page information and retry from root page to search
			// while ppdb doesn't update the parent page (for simplicity), and just retry

			m.bm.ReleaseBufferFSM(bufID, exclusive)
			bufID, err = m.bm.ReadBufferFSM(rel, page.PageID(fsmRootPageID), exclusive)
			if err != nil {
				return page.InvalidPageID, errors.Wrap(err, "ReadBufferFSM failed 3")
			}
			p = m.bm.GetPage(bufID)
			idx = rootNodeIndex
			addr = address{
				treeLevel:     treeLevelRoot,
				logicalPageID: firstLogicalPageID,
			}
		}
	}
}

// UpdateFSM updates the free space size with relation's page id.
// the location(fsm slot) can be calculated from relation's page id.
// at first update the free space size of relation's page id, then bubble up the change up to root node.
// see https://github.com/postgres/postgres/blob/bfcf1b34805f70df48eedeec237230d0cc1154a6/src/backend/storage/freespace/freespace.c#L800
func (m *Manager) UpdateFSM(rel common.Relation, pageID page.PageID, size int) error {
	// at first, fetch the fsm page which stores the free space size of the relation's page
	// find the address and slot from relation's page id
	addr, slot := getAddressFromRelationPageID(relationPageID(pageID))
	// get fsm page id from address and read the page into buffer
	fsmPageID := getFSMPageIDFromAddress(addr)

	exclusive := true
	bufID, err := m.bm.ReadBufferFSM(rel, page.PageID(fsmPageID), exclusive)
	if err != nil {
		return errors.Wrap(err, "ReadBufferFSM failed 1")
	}
	p := m.bm.GetPage(bufID)

	// convert size into free space size(1 byte stored in each fsm tree node)
	updatedSize, ok := convertToFreeSpaceSize(size)
	if !ok {
		return errors.Errorf("the size passed is unexpected: %d", size)
	}
	// update the free space size of relation's page
	idx := getNodeIndexFromSlot(slot)
	// update the free space size
	updateFreeSpaceSizeFromNodeIndex(p, idx, updatedSize)
	// then run loop for bubbling up the update
	for {
		idx = getParentNode(idx)
		fss := getFreeSpaceSizeFromNodeIndex(p, idx)
		if fss > updatedSize {
			// if the free space size of parent node is bigger than the size updated,
			// then doesn't need to update the free space and complete the update flow

			// release buffer
			m.bm.ReleaseBufferFSM(bufID, exclusive)
			return nil
		}
		// update the free space size
		updateFreeSpaceSizeFromNodeIndex(p, idx, updatedSize)
		// mark the buffer dirty
		m.bm.MarkDirty(bufID)

		/*
			patterns:
			- if the node is root node
			  - and tree level is root, complete the update and return
			  - and tree level isn't root, go up the next tree level, then fetch the fsm page and continue updating
			- if the node isn't root node
			  - continue to bubble up the update within page
		*/
		if !isRoot(idx) {
			continue
		}

		// when the tree level is root, complete updating and just return
		if addr.treeLevel == treeLevelRoot {
			// release buffer
			m.bm.ReleaseBufferFSM(bufID, exclusive)
			return nil
		}

		// release buffer and fetch the next fsm page
		m.bm.ReleaseBufferFSM(bufID, exclusive)

		// go up the tree level, fetch another fsm page
		parentAddr, parentSlot, ok := getParentAddress(addr)
		if !ok {
			return errors.Errorf("getParentAddress is unexpected: %v", addr)
		}
		addr = parentAddr
		idx = getNodeIndexFromSlot(parentSlot)
		fsmPageID := getFSMPageIDFromAddress(addr)
		// fetch new fsm page into buffer
		bufID, err = m.bm.ReadBufferFSM(rel, page.PageID(fsmPageID), exclusive)
		if err != nil {
			return errors.Wrap(err, "ReadBufferFSM failed 2")
		}
		p = m.bm.GetPage(bufID)

		// update the free space size
		updateFreeSpaceSizeFromNodeIndex(p, idx, updatedSize)
		// mark the buffer dirty
		m.bm.MarkDirty(bufID)
	}
}
