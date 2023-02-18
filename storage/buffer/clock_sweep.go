/*
Postgres adopts clock sweep as cache replacement policy on main shared buffer, so does ppdb.
Clock sweep is approximation of LRU algorithm.
The main difference is that the clock sweep does not maintain global timestamp.
It uses approximation of timestamp, usage count.
Clock sweep is better than LRU in terms of concurrency.

for more details, see https://github.com/postgres/postgres/blob/master/src/backend/storage/buffer/README#L155-L246
*/
package buffer

import (
	"sync/atomic"
)

// clockSweepTick moves clock hand ahead (increments nextVictimBuffer)
// clock sweep treas buffer pool as ring buffer
// see https://github.com/postgres/postgres/blob/24d2b2680a8d0e01b30ce8a41c4eb3b47aca5031/src/backend/storage/buffer/freelist.c#L113
func (m *Manager) clockSweepTick() BufferID {
	nextVictimBuffer := atomic.AddInt32((*int32)(&m.nextVictimBuffer), 1)
	if nextVictimBuffer >= bufferNum {
		victim := nextVictimBuffer % bufferNum
		if victim == 0 {
			for {
				wrapped := nextVictimBuffer % bufferNum
				if ok := atomic.CompareAndSwapInt32((*int32)(&m.nextVictimBuffer), nextVictimBuffer, wrapped); ok {
					break
				}
				nextVictimBuffer++
			}
		}
		return BufferID(victim)
	}
	return BufferID(nextVictimBuffer)
}

// allocateWithClockSweep decides victim buffer and pin it and return it
// postgres moves the clock hand around one cycle, and
// if all buffers cannot be evicted, return invalid buffer id.
// (although it may be too fast to return error)
// see: https://github.com/greenplum-db/gpdb/blob/abdcb97df1747bf7413918d1601ce0be8c1e6a49/src/backend/storage/buffer/freelist.c#L201
// IMPORTANT: the returned buffer is header-locked for preventing being pinned by other goroutine after the victim is decided
func (m *Manager) allocateWithClockSweep() BufferID {
	// when tryCounter is 0, it means clock sweep has inspected all buffers
	tryCounter := bufferNum
	for {
		victimBufferID := m.clockSweepTick()

		desc := m.descriptors[victimBufferID]
		// acquire buffer header lock
		// other goroutines cannot pin the buffer afterward
		desc.acquireHeaderLock()
		if refc := desc.referenceCount(); refc != 0 {
			desc.releaseHeaderLock()
			// this buffer has been referenced by other goroutines, so must not be evicted
			// decrement tryCounter and if it is 0, all buffers may be referenced
			tryCounter--
			if tryCounter == 0 {
				break
			}
			continue
		}
		if usec := desc.usageCount(); usec != 0 {
			// this buffer was used after clock sweep had inspected previous time, so must not evict it
			desc.decrementUsageCount()
			desc.releaseHeaderLock()
			// reset try counter
			tryCounter = bufferNum
			continue
		}
		// here, ref count and usage count is 0, so this buffer can be evicted
		// IMPORTANT: buffer header lock must not be released so that other goroutines cannot pin even after this function is returned
		return victimBufferID
	}
	return InvalidBufferID
}
