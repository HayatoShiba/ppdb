/*
Buffer descriptor stores metadata about each buffer.

Metadata in descriptor for cache replacement policy:
Postgres adopts clock sweep algorithm for cache replacement policy, so does ppdb.
Descriptor has three fields for the cache replacement policy:

1. pin count (or may be called ref count)
- This is used to grasp whether the buffer is now referred by other goroutines.
- If the buffer has been pinned, then the buffer cannot be evicted.
- So the flow is: pin the buffer (via ReadBuffer())-> do anything with the buffer
- -> unpin the buffer (via ReleaseBuffer()) after the process is completed.
- IMPORTANT: the caller is responsible for ReleaseBuffer() and unpin the buffer

2. usage count
- This is used to grasp whether the buffer is used after clock-sweep inspected the buffer previous time.
- If usage count is 0, then the buffer is considered as not-frequently-used so it can be evicted.
- Usage count is decremented when clock-sweep inspects the buffer.
- So the flow is: increments usage count -> use the buffer (and completes)
- -> clock-sweep decrements the usage count (when inspected).

3. dirty bit
- This is used to grasp whether the page in buffer is updated and not written out to disk yet.
- When clock-sweep tries to evict the buffer, if it is dirty,
- the buffer must be written to disk before evicted.
- This is related with buffer pool manager policy `steal` so, for more details, see /storage/buffer/manager.go

------

About state field:
State is uint32 and consists of ref count, usage count, flags which indicates buffer state.
- 18 bits: reference count
- 4 bits: usage count
- 10 bits: flags

ref count, usage count and flags are combined into one field uint32
So that all information can be updated atomic at once without holding any lock.
This can be achieved with spin lock(cas operation).
Pin/unpin is used frequently so that atomic operation instead of holding any lock can improve performance.

Flags in state field includes header lock bit and io in progress bit(kind of lock).
Header lock has to be held before changing state/tag field in descriptor.
When header lock is not held by any other goroutines,
the state field can be updated atomic with cas operation.
to summarize,
- acquire header lock or use cas operation to update state field
- when header lock is held by other goroutines, cas operation must not be executed

-----
when buffer is pinned by other goroutines, tag must not be updated

see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L199-L227
see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L30-L39
the sentence below is about CPU cache line, and it is interesting (TO READ)
https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L234-L236
*/
package buffer

import (
	"sync"
	"sync/atomic"
)

// descriptor is buffer descriptor
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L196-L254
type descriptor struct {
	// buffer tag
	tag tag
	// next free buffer id. this is free list for buffer
	nextFreeID BufferID
	// state field. see the comment at the head of this file
	state uint32
	// contentLock for protecting the buffer content read/write
	// in postgres, content lock is defined with LWLock
	// for more details, see the comment at the head of /storage/buffer/manager.go
	contentLock sync.RWMutex
}

// newDescriptors initializes descriptors for manager
// this function is expected to be called only in NewManager() and test
func newDescriptors() [bufferNum]*descriptor {
	descs := [bufferNum]*descriptor{}
	for i := 0; i < bufferNum; i++ {
		descs[i] = &descriptor{
			nextFreeID: BufferID(i + 1),
		}
	}
	descs[bufferNum-1].nextFreeID = freeListInvalidID
	return descs
}

// for flags in state field
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L58-L67
const (
	// bmLocked indicates buffer header is locked
	bmLocked uint32 = (1 << 9)
	// bmDirty indicates buffer is dirty
	bmDirty uint32 = (1 << 8)
	// bmIOInProgress indicates the io is in progress for the buffer
	// this is kind of lock for disk io
	// see https://github.com/postgres/postgres/blob/d87251048a0f293ad20cc1fe26ce9f542de105e6/src/backend/storage/buffer/README#L148-L152
	bmIOInProgress uint32 = (1 << 7)

	// other bits will be defined when necessary
)

// acquireHeaderLock acquires buffer header spin lock
// to change state/tag field in descriptor
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L4755
func (desc *descriptor) acquireHeaderLock() {
	for {
		oldState := atomic.LoadUint32(&desc.state)
		if oldState&bmLocked != 0 {
			// if header lock is held by other goroutines, just continue
			// in postgres, delay spin lock here
			// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L4769
			// maybe runtime.Gosched() should be called here
			// re-scheduling of goroutine may be controversial because the lock is expected to be released soon
			continue
		}
		newState := oldState | bmLocked
		if atomic.CompareAndSwapUint32(&desc.state, oldState, newState) {
			// if swapped, return
			break
		}
	}
}

// releaseHeaderLock releases buffer header spin lock
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L359
func (desc *descriptor) releaseHeaderLock() {
	// postgres executes pg_write_barrier() in UnlockBufHdr()
	// probably, this is for confirming completion of all other operations which update state/tag
	// because it could be critical if, after one goroutine acquires header lock, the update of state/tag field by other goroutine is completed
	// for more details about memory barrier, see https://github.com/postgres/postgres/blob/2ded19fa3a4dafbae80245710fa371d5163bdad4/src/backend/storage/lmgr/README.barrier#L1
	// in golang, I assume atomic.LoadUint32 works as kind of memory barrier (although I'm not sure...)
	state := atomic.LoadUint32(&desc.state)
	// releaseHeaderLock() is based on the fact that the caller has held header lock,
	// so does not check old value of state field here.
	atomic.SwapUint32(&desc.state, state & ^bmLocked)
}

// waitHeaderLockReleased waits for buffer header spin lock to be released
// this function is expected to be called when using CAS loops
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L4784
func (desc *descriptor) waitHeaderLockReleased() uint32 {
	var state uint32
	for {
		state = atomic.LoadUint32(&desc.state)
		// if lock is not held by other goroutine, return
		if state&bmLocked == 0 {
			break
		}
		// maybe runtime.Gosched() should be called here
		// re-scheduling of goroutine may be controversial because the lock is expected to be released soon
	}
	return state
}

// setDirty sets the dirty bit with cas operation
// this can be called without holding header lock
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L1583
func (desc *descriptor) setDirty() {
	// if the buffer is already dirty, just return
	if desc.isDirty() {
		return
	}
	for {
		oldState := atomic.LoadUint32(&desc.state)
		// wait buffer header unlocked if it is locked
		if oldState&bmLocked != 0 {
			// wait header lock to be released
			// because cas operation must not be executed when header lock held by other goroutine
			// see the comment at the head of /storage/buffer/manager.go
			// then update oldState with the state when header lock is released
			oldState = desc.waitHeaderLockReleased()
		}
		newState := oldState | bmDirty
		if atomic.CompareAndSwapUint32(&desc.state, oldState, newState) {
			// if swapped, return
			break
		}
	}
}

// clearDirty clears the dirty bit
func (desc *descriptor) clearDirty() {
	// if the buffer is not dirty, just return
	if !desc.isDirty() {
		return
	}
	for {
		oldState := atomic.LoadUint32(&desc.state)
		// wait buffer header unlocked if it is locked
		if oldState&bmLocked != 0 {
			// wait header lock to be released
			// because cas operation must not be executed when header lock held by other goroutine
			// see the comment at the head of /storage/buffer/manager.go
			// then update oldState with the state when header lock is released
			oldState = desc.waitHeaderLockReleased()
		}
		newState := oldState & ^bmDirty
		if atomic.CompareAndSwapUint32(&desc.state, oldState, newState) {
			// if swapped, return
			break
		}
	}
}

// isDirty checks whether the descriptor is dirty
func (desc *descriptor) isDirty() bool {
	state := atomic.LoadUint32(&desc.state)
	if state&bmDirty != 0 {
		return true
	}
	return false
}

// setIOInProgress sets buffer io in progress
func (desc *descriptor) setIOInProgress() {
	for {
		oldState := atomic.LoadUint32(&desc.state)
		// wait buffer header unlocked if it is locked
		if oldState&bmLocked != 0 {
			// wait header lock to be released
			// because cas operation must not be executed when header lock held by other goroutine
			// see the comment at the head of /storage/buffer/manager.go
			// then update oldState with the state when header lock is released
			oldState = desc.waitHeaderLockReleased()
		}
		// if the buffer io is in progress by other goroutine, it has to be waited
		if desc.isIOInProgress() {
			// is this released soon in most cases?
			continue
		}
		newState := oldState | bmIOInProgress
		if atomic.CompareAndSwapUint32(&desc.state, oldState, newState) {
			// if swapped, return
			break
		}
	}
}

// clearIOInProgress clears buffer io in progress
func (desc *descriptor) clearIOInProgress() {
	if !desc.isIOInProgress() {
		return
	}
	for {
		oldState := atomic.LoadUint32(&desc.state)
		// wait buffer header unlocked if it is locked
		if oldState&bmLocked != 0 {
			// wait header lock to be released
			// because cas operation must not be executed when header lock held by other goroutine
			// see the comment at the head of /storage/buffer/manager.go
			// then update oldState with the state when header lock is released
			oldState = desc.waitHeaderLockReleased()
		}
		newState := oldState & ^bmIOInProgress
		if atomic.CompareAndSwapUint32(&desc.state, oldState, newState) {
			// if swapped, return
			break
		}
	}
}

// isIOInProgress checks whether the buffer io is in progress
func (desc *descriptor) isIOInProgress() bool {
	state := atomic.LoadUint32(&desc.state)
	if state&bmIOInProgress != 0 {
		return true
	}
	return false
}
