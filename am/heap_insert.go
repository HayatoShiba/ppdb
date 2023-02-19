package am

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/buffer"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/storage/vm"
	"github.com/HayatoShiba/ppdb/transaction"
	"github.com/pkg/errors"
)

// HeapInsertInfo is information necessary for inserting heap tuple
type HeapInsertInfo struct {
	// rel indicates what relation(table) the tuple belongs to
	rel common.Relation
	// tupleData is the tuple's data which will be inserted
	tupleData []byte
}

func NewHeapInsertInfo(rel common.Relation, data []byte) HeapInsertInfo {
	return HeapInsertInfo{
		rel:       rel,
		tupleData: data,
	}
}

/*
	the logic to insert tuple
	- prepare tuple for insert
	  - set xmin with current transaction id
	  - afterwards the operation below is necessary
	    - set ctid with the self pageID, slotIndex
	    - set infomask that xmin is committed when the transaction is committed?
	- search free space map for enough size to insert
	  - if no enough space in the existing page, extend the page
	- read the page into buffer with exclusive content lock
	- add the tuple to the page
	- mark buffer dirty
	- release buffer
	- update free space map
	- update visibility map

HeapInsert heap insert
https://github.com/postgres/postgres/blob/8e1db29cdbbd218ab6ba53eea56624553c3bef8c/src/backend/access/heap/heapam_handler.c#L241
https://github.com/postgres/postgres/blob/4b3e37993254ed098219e62ceffb1b32fac388cb/src/backend/executor/nodeModifyTable.c#L708
*/
func (m *Manager) HeapInsert(hinfo HeapInsertInfo, tx transaction.Tx) error {
	tup := tuple.NewTuple(tx.ID(), hinfo.tupleData)
	size := len(tup)

	// get buffer. this buffer is pinned and acquires content exclusive lock
	// TODO: is this conversion of uint32 into int dangerous? can this result in overflow?
	bufID, pageID, err := m.getBufferForInsert(hinfo.rel, size)
	if err != nil {
		return errors.Wrap(err, "getBufferForInsert failed")
	}
	p := m.bm.GetPage(bufID)
	// put heap tuple
	if err := insertTuple(p, tup, pageID); err != nil {
		return errors.Wrap(err, "insertTuple failed")
	}

	// if the page is `all visible`, then clear bit on visibility map.
	// this checks page info flag so this has to be done here (probably)
	if page.IsAllVisible(p) {
		// clear the `all visible` bit
		// this clears the bit in the page, not visibility map
		// this enables us to check visibility information without checking visibility map
		// this is the benefit to store the bit in the page (I think.....)

		page.ClearAllVisible(p)
		// update visibility map
		// this clears also frozen flag if it is set
		m.bm.UpdateVMStatus(hinfo.rel, pageID, vm.StatusInitialized)
	}

	// mark buffer dirty
	m.bm.MarkDirty(bufID)
	m.bm.ReleaseContentLock(bufID, true)
	// wal log
	m.bm.ReleaseBuffer(bufID)
	return nil
}

// getBufferForInsert gets buffer for insertion of the tuple
// this returns pinned and exclusive content locked buffer
// TODO: consider concurrency https://github.com/postgres/postgres/blob/2dc2e4e31adb71502074c8c2bf9e0766347aa6e5/src/backend/access/heap/hio.c#L283-L302
// https://github.com/postgres/postgres/blob/2dc2e4e31adb71502074c8c2bf9e0766347aa6e5/src/backend/access/heap/hio.c#L333
func (m *Manager) getBufferForInsert(reln common.Relation, tupleSize int) (buffer.BufferID, page.PageID, error) {
	// at first, search with free space map
	pageID, err := m.fsm.SearchPageIDWithFreeSpaceSize(reln, tupleSize)
	if err != nil {
		return buffer.InvalidBufferID, pageID, errors.Wrap(err, "SearchPageIDWithFreeSpaceSize failed")
	}

	for {
		// when no free space in existing page, extend page
		if pageID == page.InvalidPageID {
			// when pass NewPageID to ReadBuffer, page must be extended
			pageID = page.NewPageID
		}
		bufID, err := m.bm.ReadBuffer(reln, disk.ForkNumberMain, pageID)
		if err != nil {
			return buffer.InvalidBufferID, pageID, errors.Wrap(err, "ReadBuffer failed")
		}
		// acquire exclusive content lock
		m.bm.AcquireContentLock(bufID, true)
		// when page has not been initialized, initialize it
		p := m.bm.GetPage(bufID)
		if !page.IsInitialized(p) {
			// TODO: fix specialSpaceSize
			page.InitializePage(p, 10)
			m.bm.MarkDirty(bufID)
		}

		// check free space size
		fss := page.CalculateFreeSpace(p)
		if fss >= tupleSize {
			// when enough free space, just return
			return bufID, pageID, nil
		}
		// if there is not enough free space in page, release content lock and unpin and release buffer
		// then continue to search enough space
		// TODO: maybe free space map information should be updated?
		m.bm.ReleaseContentLock(bufID, true)
		m.bm.ReleaseBuffer(bufID)
		// try to search with free space map one more time
		pageID, err = m.fsm.SearchPageIDWithFreeSpaceSize(reln, tupleSize)
		if err != nil {
			return buffer.InvalidBufferID, pageID, errors.Wrap(err, "SearchPageIDWithFreeSpaceSize failed")
		}
	}
}

// insertTuple inserts tuple into the buffer
// https://github.com/postgres/postgres/blob/2dc2e4e31adb71502074c8c2bf9e0766347aa6e5/src/backend/access/heap/hio.c#L36
func insertTuple(p page.PagePtr, tup tuple.TupleByte, pageID page.PageID) error {
	nidx := page.GetNSlotIndex(p)
	// TODO: this is not unclear logic... should refactor.
	// GetNSlotIndex() may return InvalidSlotIndex and AddItem() is also considered.
	var tid tuple.Tid
	if nidx != page.InvalidSlotIndex {
		// the slot index where the tuple will be inserted
		// TODO: if free slot exists, use it
		nidx = nidx + 1
		// update the tuple's ctid to this position(pageID, slotIndex)
		tid = tuple.NewTid(pageID, nidx)
	} else {
		// this doesn't increment nidx. TODO: refactor. this is really unclear
		// update the tuple's ctid to this position(pageID, slotIndex)
		tid = tuple.NewTid(pageID, page.FirstSlotIndex)
	}

	insertSlotIdx := nidx

	tup.SetCtid(tid)

	if err := page.AddItem(p, page.ItemPtr(tup), insertSlotIdx); err != nil {
		return errors.Wrap(err, "page.AddItem")
	}
	return nil
}
