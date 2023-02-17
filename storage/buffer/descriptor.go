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

// descriptor is buffer descriptor
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L196-L254
type descriptor struct {
	// buffer tag
	tag tag
	// next free buffer id. this is free list for buffer
	nextFreeID BufferID
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
