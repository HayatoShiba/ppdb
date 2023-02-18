/*
the implementation of free list

In ppdb, if buffer is removed from free list, the buffer will be never added to free list.
So this free list thing doesn't make much sense in ppdb.
*/
package buffer

const (
	// this indicates the end of the free list
	freeListInvalidID BufferID = -1
)

// allocateFromFreeList returns buffer from free list.
// this removes the buffer from free list.
// if there is no buffer in free list, just return InvalidBufferID
func (m *Manager) allocateFromFreeList() BufferID {
	// check the first node without acquiring buffer strategy lock.
	// if it isn't invalid id, then acquire lock and re-check the first node and return it.
	// this is kind of optimistic locking.
	if m.freeList == freeListInvalidID {
		return freeListInvalidID
	}

	// acquire strategy lock
	m.strategyLock.Lock()
	bufID := m.freeList
	// re-check after acquire lock
	if bufID == freeListInvalidID {
		return freeListInvalidID
	}

	desc := m.descriptors[bufID]
	// remove first buffer from free list
	m.freeList = desc.nextFreeID
	m.strategyLock.Unlock()
	return bufID
}
