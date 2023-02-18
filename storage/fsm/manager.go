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
	"github.com/HayatoShiba/ppdb/storage/buffer"
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
