/*
Dirty pages have to be written out to disk before evicted.
If disk IO happens when page is read, it is not good in terms of performance.
So background writing is introduced.
Background writer periodically checks whether buffer is dirty, and
if it's dirty, the writer writes out the dirty buffer to disk ahead of time.

for parameters defined in postgres, see 20.4.5 in the link below.
https://www.postgresql.org/docs/current/runtime-config-resource.html#RUNTIME-CONFIG-RESOURCE-BACKGROUND-WRITER
*/
package buffer

import (
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	// delay between active rounds
	// default is 200ms in postgres
	bgWriterDelay = 10000
	// in each round, 100 buffers are flushed at most
	// see https://www.postgresql.org/docs/current/runtime-config-resource.html
	bgWriterMaxPages = 100
)

type BackgroundWriter struct {
	m *Manager
}

// BackgroundWrite is background writing
// this function flushes dirty buffer on background periodically
// postgres implements in more complicated way
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L2224
func (bw *BackgroundWriter) Run() error {
	writtenPages := 0
	for {
		// check starts from the next victim buffer
		nextVictimBuffer := atomic.LoadInt32((*int32)(&bw.m.nextVictimBuffer))
		victimID := BufferID(nextVictimBuffer % bufferNum)
		// check all buffers by default
		for i := 0; i < bufferNum; i++ {
			// syncOneBuffer checks whether the buffer is dirty, and if dirty, just return.
			written, err := bw.m.syncOneBuffer(victimID)
			if err != nil {
				return errors.Wrap(err, "syncOneBuffer failed")
			}
			// if flushed, increment writtenPages
			if written {
				writtenPages++
				// if write max pages, stop bgwriter
				if writtenPages >= bgWriterMaxPages {
					break
				}
			}
			// check next buffer
			victimID++
			victimID = victimID % bufferNum
		}
		// sleep in each round
		time.Sleep(bgWriterDelay * time.Millisecond)
	}
}

// syncOneBuffer flushes the buffer into disk
// this is called by checkpointer, bgwriter
// ppdb returns simple result, the buffer is flushed or not
// postgres returns additionally whether the buffer is reusable or not
// see https://github.com/postgres/postgres/blob/d9d873bac67047cfacc9f5ef96ee488f2cb0f1c3/src/backend/storage/buffer/bufmgr.c#L2528
func (m *Manager) syncOneBuffer(bufID BufferID) (bool, error) {
	desc := m.descriptors[bufID]
	desc.acquireHeaderLock()
	if !desc.isDirty() {
		// if the buffer is not dirty, don't have to do anything
		return false, nil
	}

	// check whether the buffer is reusable
	// if (desc.referenceCount() == 0) && (desc.usageCount() == 0) {
	// 	result |= syncResultReusable
	// }

	// when flushBuffer is called, the caller has to hold pin and shared content lock
	// here, pin() cannot be called because of holding header lock
	desc.pinWithHeaderLock()
	desc.contentLock.RLock()
	if err := m.flushBuffer(bufID); err != nil {
		return false, errors.Wrap(err, "m.flushBuffer failed")
	}
	desc.contentLock.RUnlock()
	desc.unpin()

	return true, nil
}
