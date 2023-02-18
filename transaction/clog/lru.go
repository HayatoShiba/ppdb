package clog

import (
	"sync/atomic"

	"github.com/HayatoShiba/ppdb/storage/page"
)

type lruInfo struct {
	// currLRUCount is lru count lastly allocated
	// this is used as kind of timestamp
	currLRUCount uint64
	// latestPageID is the biggest page id and the page is not evicted by lru
	// https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/access/slru.h#L93-L98
	latestPageID page.PageID
}

const (
	firstLRUCount uint64 = 1
)

// selectVictimBuffer returns victim buffer id
// https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L1041-L1116
func (bm *bufferManager) selectVictimBuffer() bufferID {
	victimBufferID := firstBufferID
	victimLRUCount := bm.descriptors[firstBufferID].lruCount
	for i := victimBufferID; i < bufferNum; i++ {
		// if the buffer has not been used, return it
		if bm.descriptors[i].status == bufferStatusEmpty {
			// ここでdescriptorのlockとってないと返した後にread io走るとかあるのでは
			return i
		}
		if bm.descriptors[i].status == bufferStatusReadIOInProgress ||
			bm.descriptors[i].status == bufferStatusWriteIOInProgress {
			// TODO: wait lock and re-check this id
			continue
		}
		// if it is latest page, skip it because write operation will mostly happen to the page
		if bm.descriptors[i].pageID == bm.lru.latestPageID {
			continue
		}

		if bm.descriptors[i].lruCount < victimLRUCount {
			victimBufferID = i
			victimLRUCount = bm.descriptors[i].lruCount
		}
	}
	return victimBufferID
}

// updateLRUcount updates lru count of a buffer
func (bm *bufferManager) updateLRUcount(bufID bufferID) {
	// postgres says `there are often many consecutive accesses to the same page (particularly the latest page)`
	// so postgres uses if-test for avoiding wraparound and ppdb follows it
	// see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L95-L121
	if bm.descriptors[bufID].lruCount == bm.lru.currLRUCount {
		// not update/advance lru count and just return
		return
	}
	bm.descriptors[bufID].lruCount = atomic.AddUint64(&bm.lru.currLRUCount, 1)
}
