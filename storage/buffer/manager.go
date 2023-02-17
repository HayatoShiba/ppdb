/*
Shared buffer pool manager manages buffer used for main table files/index files/fsm files/vm files.
Clog and wal is not managed by this manager (this is the same as postgres).
Disk IO is expensive so data should be cached on memory and hared buffer pool manager is responsible for this.

the implementation of buffer pool manager in ppdb is based on /src/backend/storage/buffer in postgres.
see great README: https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L1

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
	"github.com/HayatoShiba/ppdb/storage/disk"
)

// Manager manages shared buffer pool
// clog and wal is not managed by this manager
type Manager struct {
	// disk manager
	dm *disk.Manager
	// shared buffers
	buffers [bufferNum]buffer
	// descriptors of each shared buffers
	descriptors [bufferNum]*descriptor
}

// NewManager initializes the shared buffer pool manager
func NewManager(dm *disk.Manager) *Manager {
	return &Manager{
		dm:          dm,
		buffers:     newBuffers(),
		descriptors: newDescriptors(),
	}
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
