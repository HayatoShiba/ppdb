/*
Shared buffer pool manager manages buffer used for main table files/index files/fsm files/vm files.
Clog and wal is not managed by this manager (this is the same as postgres).
Disk IO is expensive so data should be cached on memory and hared buffer pool manager is responsible for this.

the implementation of buffer pool manager in ppdb is based on /src/backend/storage/buffer in postgres.
see great README: https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L1

the methods as main entry point is described below
https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L15-L30

----

Postgres adopts steal/no-force policy.
- steal: buffer pool manager is allowed to write out uncommitted change to disk.
- no-force: buffer pool manager does not force to write out change to disk before commit.

Steal policy:
The policy is beneficial when one transaction updating a tons of records larger than the buffer pool size
because, with no-steal policy, postgres cannot write out dirty pages until the transaction is committed.
But steal policy makes undo operation necessary
because, when the transaction is not committed eventually, the change written to disk has to be `undo`.

undo operation:
Postgres does not implement undo operation.
In postgres, when update/delete query is executed,
the new tuple is appended and old tuple is not updated/deleted physically
and postgres implements clog which is commit status of all transactions and clog is persistent on disk
which means that, with clog, postgres can identify which tuple on disk isn't committed and
can refer to old tuple.
This means `undo` operation is unnecessary.
for more details, see https://www.postgresql.org/docs/7.3/wal.html

no-force policy:
The policy improves performance thanks to no random IO, but this makes redo operation necessary.
Postgres implements wal so it can process `redo`, so no-force is no problem.

---

access rules for buffers:
there are two important access rules
- pin/unpin for cache eviction policy: see /storage/buffer/descriptor.go
- content locks for read/write page within buffer

the flow when scan and get the tuples on the buffer is described below:
- pin the buffer -> acquire content lock (probably shared lock) -> scan and get the tuples
- -> release content lock -> unpin the buffer

the flow when update/insert the tuples is described below:
- pin the buffer -> acquire exclusive content lock -> update/insert the tuples
- -> release content lock -> unpin the buffer
- this can prevent other goroutine from seeing partially updated data
- which means doing anything with the buffer is atomic to any other goroutines

TO READ(I haven't read yet):
update tuple commit status bits
https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L58-L70
physically remove a tuple or compact free space on a page
https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L72-L81
LockBufferForCleanup
https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L84-L97

see for more details: https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L37-L97

-----

# The list of locks used for buffer

- buffer content lock:
  - this protects each buffer content(page)
  - this is implemented with LWLock because lock may be held long time (for doing anything with the content)

- buffer header lock:
  - this protects each buffer header
  - this is implemented with spin lock because, in most cases, only a few operations are executed at one time

- BM_IO_IN_PROGRESS flag:
  - this is kind of lock for each buffer IO

- buffer strategy lock (system-wide lock):
  - this protects free-list / select victim buffer for replacement

- buffer mapping lock
  - this protects partitioned backet of mapping (mapping from buffer tag to buffer id)

- pin/unpin

see for more details: https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L100-L152

------

buffer replacement
The flow for finding the free(victim) buffer is described below
- acquire strategy lock for entering free list
  - if node exists in free list, then removes it from free list, and release lock
  - if the pin count or usage count is not 0, then leaves it and
  - re-acquires lock and enters free list again. (although I'm not sure when this happens)

- if node does not exist in free list, uses clock-sweep and get the next victim buffer
  - (ppdb does not acquire strategy lock when using clock-sweep while postgres does it)
  - if the next victim buffer is pinned or has been used, then decrement usage count and skip it
  - if the next victim buffer isn't pinned and hasn't been used, then pin the buffer and evict it
  - (although I'm not sure, after pinning the buffer, other goroutine can pin the buffer and
  - use the buffer after evicted and fetched another page into the buffer.)
  - when the buffer is dirty, then the buffer has to be written to disk before eviction

note:
I've heard that database manages in-memory cache INSTEAD OF OS
because database can manages cache on-memory better than OS and duplicated cache is inefficient.
The example of `better than OS` is
database has the information about which page is in use from client
so that it can implement better cache eviction algorithm more appropriate for database.
However, postgres uses both OS IO buffer and postgres's shared buffer, which is duplicate management and inefficient.
this is surprising.
*/
package buffer

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
)

// Manager manages shared buffer pool
// clog and wal is not managed by this manager
type Manager struct {
	// disk manager
	dm *disk.Manager
	// table is mapping from buffer tag to buffer id(index of buffers/descriptors)
	// so to read buffer, prepare tag and use the tag to get buffer id through buffer table
	table bufferTable
	// shared buffers
	buffers [bufferNum]buffer
	// descriptors of each shared buffers
	descriptors [bufferNum]*descriptor
	// freeList points to the head node(free buffer) of free list
	// this is protected by buffer strategy lock
	freeList BufferID
	// nextVictimBuffer is the next victim buffer which clock-sweep inspects
	// if the buffer is not pinned and used at that time, the buffer is confirmed to be next victim
	nextVictimBuffer BufferID
	// strategyLock is buffer strategy lock for free list
	// in postgres, this also protects clock-sweep algorithm, but ppdb protects only free list(probably)
	strategyLock sync.Mutex
}

// NewManager initializes the shared buffer pool manager
func NewManager(dm *disk.Manager) *Manager {
	return &Manager{
		dm: dm,
		table: bufferTable{
			table: make(map[tag]BufferID),
		},
		buffers:          newBuffers(),
		descriptors:      newDescriptors(),
		freeList:         FirstBufferID,
		nextVictimBuffer: FirstBufferID,
	}
}

/*
ReadBuffer returns the id of buffer where the page the caller is looking for exists.
the returned buffer has been pinned so the caller has to call ReleaseBuffer() after completion of using the buffer.

when the page is already stored within a buffer, just return it.
when the page is not, then fetch the page from disk into buffer and return it.
when the caller wants new (extended) page , pass page.NewPageID as pageID arg.

see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L717-L759
*/
func (m *Manager) ReadBuffer(rel common.Relation, forkNum disk.ForkNumber, pageID page.PageID) (BufferID, error) {
	newTag := tag{
		rel:     rel,
		forkNum: forkNum,
		pageID:  pageID,
	}
	var err error
	// if pageID passed is NewPageID, extend page and return it
	if pageID == page.NewPageID {
		pageID, err = m.dm.ExtendPage(newTag.rel, newTag.forkNum, false)
		if err != nil {
			return InvalidBufferID, errors.Wrap(err, "dm.ExtendPage failed")
		}
		// update tag page id
		newTag.pageID = pageID
	}

	m.table.RLock()
	// check whether the tag already exists in the buffer table. if it exists, just return it
	if bufID, ok := m.table.table[newTag]; ok {
		// if found, return the buffer id after pin buffer and unlock hash table
		// pin is necessary for preventing eviction
		// TODO: before pin() is called, can be the buffer evicted?
		m.descriptors[bufID].pin()
		m.table.RUnlock()
		return bufID, nil
	}
	// unlock the buffer table lock. we don't need it anymore
	m.table.RUnlock()

	var desc *descriptor
	var bufID BufferID
	for {
		// allocateBuffer() searches free list at first, then if not found, it uses clock sweep
		// header lock of allocated buffer is held for preventing pinned by other goroutine
		bufID, err = m.allocateBuffer()
		if err != nil {
			return InvalidBufferID, errors.New("allocateBuffer failed")
		}
		desc = m.descriptors[bufID]
		// pin() cannot be used here because the caller holds header lock
		// so pinWithHeaderLock() should be used. this function releases header lock and acquires pin
		// although I'm not sure why postgres releases header lock here...
		// later, re-acquire header lock and check ref count and dirty bit for other goroutines to hold pin or update the content after here
		// but holding header lock and not releasing it is more efficient? probably I miss something
		if err := desc.pinWithHeaderLock(); err != nil {
			return InvalidBufferID, errors.Wrap(err, "pinWithHeaderLock failed")
		}

		// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L1213-L1292
		if desc.isDirty() {
			// if the buffer is dirty, it must be written out to disk before eviction
			// this adds overhead so `background writing` improves performance. background writer exists in postgres.

			// in postgres, when some condition is met, it gives up using the buffer and resume to select next victim
			// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L1256-L1263

			// for preventing update of the page by other goroutine, acquire shared content lock
			m.AcquireContentLock(bufID, false)
			m.flushBuffer(bufID)
			m.ReleaseContentLock(bufID, false)

			// postgres doesn't clear dirty bit:
		}

		// postgres acquires lower-numberd partition lock first to avoid deadlocks
		// ppdb does not provide partition lock, just single global lock. so ppdb does not implement this logic. (but interesting, so note it here.)
		// https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L1309-L1327

		// acquire global lock for buffer table
		m.table.Lock()
		// insert new entry
		m.table.table[newTag] = bufID

		desc.acquireHeaderLock()
		// if pin is held by other goroutines or the buffer has been updated, resume to select next victim buffer
		if (desc.referenceCount() == 1) || !(desc.isDirty()) {
			// good! this buffer can be evicted and now holding the header lock so other goroutines cannot do anything including pin
			break
		}
		// give up the buffer and continue
		desc.releaseHeaderLock()
		// delete the inserted entry
		delete(m.table.table, newTag)
		m.table.Unlock()
		desc.unpin()
	}

	// here, buffer table is locking and the buffer header lock is held and the buffer is pinned
	// delete the old buffer tag entry from buffer table
	delete(m.table.table, desc.tag)

	// postgres releases buffer header lock then deletes the old entry from buffer table
	// but is it correct? do we have to delete the old entry at first to prevent other goroutines from entering this buffer for old entry? I'm not sure....
	desc.releaseHeaderLock()
	m.table.Unlock()

	// probably here, content lock doesn't have to be acquired because no problem with the update of page? (I've read somewhere like this, but I'm not sure...)
	desc.setIOInProgress()
	// read page into the buffer
	if err := m.dm.ReadPage(newTag.rel, newTag.forkNum, pageID, page.PagePtr(m.buffers[bufID][:])); err != nil {
		return InvalidBufferID, errors.Wrap(err, "dm.ReadPage failed")
	}
	desc.clearIOInProgress()

	// reset descriptor tag
	m.descriptors[bufID].tag = newTag

	// here, the buffer has been pinned
	return bufID, nil
}

// ReleaseBuffer unpins the buffer
// when ReadBuffer() is called, it returns pinned buffer.
// so caller has to unpin the buffer after it completes using the buffer.
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L3932
func (m *Manager) ReleaseBuffer(bufID BufferID) {
	desc := m.descriptors[bufID]
	desc.unpin()
}

// flushBuffer flushes buffer into disk
// the caller must hold a pin for preventing eviction
// and also must hold shared content lock for preventing the content updated during flush.
// Additionaly, in this function, bmIOInProgress (kind of lock for io) is held during IO
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L2823
func (m *Manager) flushBuffer(bufID BufferID) error {
	desc := m.descriptors[bufID]
	// TODO: check lsn stored in the page and flush wal if needed
	// note: buffer policy in postgres is `steal` so commit is not necessary before dirty page is written out to disk.
	// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L2863-L2887

	desc.setIOInProgress()
	// write page. fsync don't have to be used since we have WAL (probably)
	if err := m.dm.WritePage(desc.tag.rel, desc.tag.forkNum, desc.tag.pageID,
		page.PagePtr(m.buffers[bufID][:]), false); err != nil {
		return errors.Wrap(err, "dm.WritePage failed")
	}
	desc.clearIOInProgress()
	return nil
}

// GetPage returns page stored at the buffer
func (m *Manager) GetPage(bufID BufferID) page.PagePtr {
	buffer := m.buffers[bufID]
	return page.PagePtr(buffer[:])
}

// allocateBuffer returns victim buffer id where the data will be read into.
// IMPORTANT: the header lock of the buffer is held
func (m *Manager) allocateBuffer() (BufferID, error) {
	// at first, search free list.
	// if free buffer exists on the list, remove it from free list and return it
	if bufferID := m.allocateFromFreeList(); bufferID != InvalidBufferID {
		// acquire header lock for the buffer
		// although this buffer has been free so header lock doesn't have to be acquired (probably)
		desc := m.descriptors[bufferID]
		desc.acquireHeaderLock()
		return bufferID, nil
	}
	// when there is no buffer in free list, use cache replacement policy(clock-sweep)
	if bufferID := m.allocateWithClockSweep(); bufferID != InvalidBufferID {
		// allocateWithClockSweep acquires header lock for the buffer
		return bufferID, nil
	}
	return InvalidBufferID, errors.New("all buffers cannot be evicted")
}

// MarkDirty turns on the dirty bit of the buffer
// the caller has to hold pin and exclusive content lock
// for example, heap access method has to call this function after inserting some tuples.
// so turning on the buffer dirty bit has to be exported.
// https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L1583
func (m *Manager) MarkDirty(bufID BufferID) {
	desc := m.descriptors[bufID]
	desc.setDirty()
}

// AcquireContentLock acquires buffer content lock
// content lock has to be held when read/write page(buffer content)
// TODO: maybe `descriptor` type should be exported
func (m *Manager) AcquireContentLock(bufID BufferID, exclusive bool) {
	desc := m.descriptors[bufID]
	if exclusive {
		desc.contentLock.Lock()
	} else {
		desc.contentLock.RLock()
	}
}

// ReleaseContentLock releases buffer content lock
// content lock has to be released after any operations to page(buffer content) is completed
// TODO: maybe descriptor should be exported
func (m *Manager) ReleaseContentLock(bufID BufferID, exclusive bool) {
	desc := m.descriptors[bufID]
	if exclusive {
		desc.contentLock.Unlock()
	} else {
		desc.contentLock.RUnlock()
	}
}
