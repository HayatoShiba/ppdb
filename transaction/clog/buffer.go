package clog

import (
	"sync"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/pkg/errors"
)

const (
	// the size of buffer. this must be equal to page size
	bufferSize = page.PageSize

	// buffer size is min between 128 or the size calculated from the size of shared buffer
	// see https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L663-L683
	// in ppdb, to simplify, just define 10
	bufferNum = 10
)

// buffer is buffer
type buffer *[bufferSize]byte

// bufferID is called slotno in postgres
type bufferID int

const (
	// invalid buffer id
	invalidBufferID bufferID = -1
	// first normal buffer id
	firstBufferID bufferID = 0
)

// bufferManager is buffer manager
type bufferManager struct {
	dm          *diskManager
	buffers     [bufferNum]buffer
	descriptors [bufferNum]*bufferDescriptor

	// lru is information used for lru
	lru lruInfo

	// lock for the whole buffer (XactSLRULock called in postgres, but I may misunderstand the lock)
	// this looks inefficient
	// note: XactSLRULock is the same lock as SlruShared.ControlLock.
	// this may be confusing to read the postgres source code.
	// see the process during initialization: https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L210
	sync.RWMutex
}

// newBufferManager initializes buffer manager
func newBufferManager(dm *diskManager) *bufferManager {
	return &bufferManager{
		dm:          dm,
		buffers:     newBuffers(),
		descriptors: newBufferDescriptors(),
		lru: lruInfo{
			currLRUCount: firstLRUCount,
		},
	}
}

// newBuffers initializes buffer pool
// this is expected to be called during initialization
func newBuffers() [bufferNum]buffer {
	var buffers [bufferNum]buffer
	for i := 0; i < bufferNum; i++ {
		buffers[i] = &[bufferSize]byte{}
	}
	return buffers
}

// getState returns transaction state from buffer
func (bm *bufferManager) getState(txID txid.TxID) (state, error) {
	pageID := getPageIDFromTxID(txID)
	byteOffset := getByteOffsetFromTxID(txID)

	// acquire reader lock -> read buffer -> get xid status -> release reader lock -> return it
	// lock is a must for confirming that the buffer read will not be evicted before
	// getting the transaction's status
	bm.RLock()

	// get the id of buffer which stores the page
	bufID, err := bm.readPage(pageID, false)
	if err != nil {
		return stateInProgress, errors.Wrap(err, "readPage failed")
	}

	st := getState(bm.buffers[bufID][byteOffset], txID)
	bm.RUnlock()
	return st, nil
}

// updateState fetches the page into buffer and update the state and mark the buffer dirty
func (bm *bufferManager) updateState(txID txid.TxID, st state) error {
	pageID := getPageIDFromTxID(txID)
	byteOffset := getByteOffsetFromTxID(txID)

	// acquire writer lock -> read buffer -> update xid status -> mark buffer dirty ->
	// release writer lock -> return it
	// lock is a must for confirming that the buffer read will not be evicted

	// acquire writer lock
	// there is optimization for contention in postgres:
	// see https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L281-L294
	bm.Lock()

	bufID, err := bm.readPage(pageID, true)
	if err != nil {
		return errors.Wrap(err, "readPage failed")
	}

	bt := bm.buffers[bufID][byteOffset]
	bm.buffers[bufID][byteOffset] = getUpdatedState(bt, txID, st)
	bm.descriptors[bufID].dirty = true

	bm.Unlock()
	return nil
}

// readPage seaches buffer. If the page is not found, then fetch it from disk and return the buffer id.
// buffer lock is expected to be held when this function is called.
// see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L496
func (bm *bufferManager) readPage(pageID page.PageID, exclusive bool) (bufferID, error) {
	// actually, in postgres, search page is executed with reader lock
	// but, in ppdb, to simplify the logic, this may be executed with writer lock
	// see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L501-L520
	// search the page in buffer
	if bufID := bm.searchPage(pageID); bufID != invalidBufferID {
		// update lru count of the buffer
		bm.updateLRUcount(bufID)
		return bufID, nil
	}

	// select victim buffer id using lru count
	victimBufferID := bm.selectVictimBuffer()
	victimID := bufferID(victimBufferID)
	// if dirty, should flush it to disk
	if bm.descriptors[victimID].dirty {
		if err := bm.flushPage(victimID, exclusive); err != nil {
			return invalidBufferID, errors.Wrap(err, "flushPage failed")
		}
	}

	bm.descriptors[victimID].status = bufferStatusReadIOInProgress
	// at first, acquire per buffer lock for io. then release the whole buffer lock
	bm.descriptors[victimID].Lock()
	// during io, release the whole buffer lock
	if exclusive {
		bm.Unlock()
	} else {
		bm.RUnlock()
	}
	if err := bm.dm.readPage(pageID, page.PagePtr(bm.buffers[victimID])); err != nil {
		return invalidBufferID, errors.Wrap(err, "readPage failed")
	}

	// at first, re-acquire the whole buffer lock. then release per buffer lock
	if exclusive {
		bm.Lock()
	} else {
		bm.RLock()
	}
	bm.descriptors[victimID].Unlock()
	bm.descriptors[victimID].status = bufferStatusUsed
	bm.descriptors[victimID].pageID = pageID
	// update lru count of the buffer
	bm.updateLRUcount(victimID)

	return victimID, nil
}

// searchPage searches buffer for page
// this function looks inefficient
// https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L1033-L1039
func (bm *bufferManager) searchPage(pageID page.PageID) bufferID {
	for i := 0; i < bufferNum; i++ {
		if bm.descriptors[i].pageID == pageID {
			if bm.descriptors[i].status == bufferStatusUsed {
				return bufferID(i)
			}
		}
	}
	return invalidBufferID
}

// flushPage flushes page into disk
// the whole buffer exclusive lock is expected to be held when this function is called
// reader lockで良いのでは？
// see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L540
func (bm *bufferManager) flushPage(bufID bufferID, exclusive bool) error {
	// if not dirty here, just return
	if !bm.descriptors[bufID].dirty {
		return nil
	}

	// これってこんな何のロックも取らずに変えて良いんだっけ
	bm.descriptors[bufID].status = bufferStatusWriteIOInProgress
	// at first, acquire per buffer lock for io. then release the whole buffer lock
	bm.descriptors[bufID].Lock()
	// during io, release the whole buffer lock
	if exclusive {
		bm.Unlock()
	} else {
		bm.RUnlock()
	}
	if err := bm.dm.writePage(bm.descriptors[bufID].pageID, page.PagePtr(bm.buffers[bufID])); err != nil {
		return errors.Wrap(err, "dm.writePage failed")
	}
	// at first, re-acquire the whole buffer lock. then release per buffer lock
	if exclusive {
		bm.Lock()
	} else {
		bm.RLock()
	}
	bm.descriptors[bufID].Unlock()
	bm.descriptors[bufID].dirty = false
	bm.descriptors[bufID].status = bufferStatusUsed
	return nil
}
