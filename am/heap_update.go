package am

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/storage/vm"
	"github.com/HayatoShiba/ppdb/transaction"
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
	"github.com/pkg/errors"
)

// HeapUpdateInfo is information necessary for updating heap tuple
type HeapUpdateInfo struct {
	// rel indicates what relation(table) the tuple belongs to
	rel common.Relation
	// tid is tuple's tid
	tid tuple.Tid

	// tupleData is the tuple's data which will be inserted(updated)
	tupleData []byte
}

func NewHeapUpdateInfo(rel common.Relation, tid tuple.Tid, data []byte) HeapUpdateInfo {
	return HeapUpdateInfo{
		rel:       rel,
		tid:       tid,
		tupleData: data,
	}
}

// HeapUpdate updates the specified tuple with the new tuple
// the process to update the tuple is `delete the old tuple` and `insert new tuple`.
// so, it must be helpful to see the logic of HeapDelete() and HeapInsert().
// the main difference is that when update the tuple, set tuple's ctid with the pointer to new tuple.
// this results in `version chain` (so-called in CMU database lecture) and this helps index behavior.
// the version chain in postgres is chained from old tuple to newer tuple, so it's easy for index to follow the chain from entry point.
// although this has disadvantage that, it the tuple has been updated many times, index has to follow the long chain to get the latest tuple.
// https://github.com/postgres/postgres/blob/8e1db29cdbbd218ab6ba53eea56624553c3bef8c/src/backend/access/heap/heapam_handler.c#L314
func (m *Manager) HeapUpdate(huinfo HeapUpdateInfo, tx transaction.Tx) (snapshot.TMResult, error) {
	// read buffer. the buffer has been pinned so this buffer cannot be evicted
	bufID, err := m.bm.ReadBuffer(huinfo.rel, disk.ForkNumberMain, huinfo.tid.PageID())
	if err != nil {
		return snapshot.TMResultInvisible, errors.Wrap(err, "ReadBuffer failed")
	}
	// acquire exclusive content lock for buffer to change
	m.bm.AcquireContentLock(bufID, true)

	p := m.bm.GetPage(bufID)
	item, err := page.GetItem(p, huinfo.tid.SlotIndex())
	if err != nil {
		return snapshot.TMResultInvisible, errors.Wrap(err, "page.GetItem failed")
	}
	// convert item into tuple type
	tup := tuple.TupleByte(item)

	tmres, err := m.sm.CanTupleBeModified(tup, huinfo.tid)
	if err != nil {
		return snapshot.TMResultInvisible, errors.Wrap(err, "sm.CanTupleBeModified failed")
	}
	switch tmres {
	case snapshot.TMResultBeingModified:
	// TODO: wait another transaction
	case snapshot.TMResultOK:
		// check additional visibility
		snap := tx.Snapshot()
		visible, err := m.sm.IsTupleVisibleFromSnapshot(tup, &snap)
		if err != nil {
			return snapshot.TMResultInvisible, errors.Wrap(err, "sm.IsTupleVisibleFromSnapshot failed")
		}
		if !visible {
			tmres = snapshot.TMResultUpdated
		}
	}

	if tmres != snapshot.TMResultOK {
		// the tuple cannot be updated
		// when the tuple is invisible/updated/deleted, just return it
		m.bm.ReleaseContentLock(bufID, true)
		m.bm.ReleaseBuffer(bufID)
		return tmres, nil
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
		m.bm.UpdateVMStatus(huinfo.rel, huinfo.tid.PageID(), vm.StatusInitialized)
	}

	// check free space size for updated tuple (newly inserted tuple)
	newtup := tuple.NewTuple(tx.ID(), huinfo.tupleData)
	newTupSize := len(newtup)
	pageID := huinfo.tid.PageID()
	// if the page has enough free space, just insert new tuple
	if newTupSize < page.CalculateFreeSpace(p) {
		// put heap tuple
		newTupTid, err := insertTuple(p, newtup, pageID)
		if err != nil {
			return tmres, errors.Wrap(err, "insertTuple failed")
		}

		// set xmax current transaction id to delete
		tup.SetXmax(tx.ID())
		// update old tuple's ctid to point to the new version tuple. this is version chain.
		// https://github.com/postgres/postgres/blob/63c844a0a5d70cdbd6ae0470d582d39e75ad8d66/src/backend/access/heap/heapam.c#L3912-L3913
		tup.SetCtid(newTupTid)

		// mark buffer dirty
		m.bm.MarkDirty(bufID)

		m.bm.ReleaseContentLock(bufID, true)
		// wal log
		m.bm.ReleaseBuffer(bufID)
		return tmres, nil
	}
	// if the page doesn't have enough free space, find it like HeapInsert()

	// get buffer. this buffer is pinned and acquires content exclusive lock
	newbufID, newpageID, err := m.getBufferForInsert(huinfo.rel, newTupSize)
	if err != nil {
		return tmres, errors.Wrap(err, "getBufferForInsert failed")
	}
	newp := m.bm.GetPage(newbufID)

	// put heap tuple
	newTupTid, err := insertTuple(newp, newtup, newpageID)
	if err != nil {
		return tmres, errors.Wrap(err, "insertTuple failed")
	}

	// set xmax current transaction id to delete
	tup.SetXmax(tx.ID())
	// update old tuple's ctid to point to the new version tuple. this is version chain.
	// https://github.com/postgres/postgres/blob/63c844a0a5d70cdbd6ae0470d582d39e75ad8d66/src/backend/access/heap/heapam.c#L3912-L3913
	tup.SetCtid(newTupTid)

	// mark buffer dirty
	m.bm.MarkDirty(bufID)

	m.bm.ReleaseContentLock(bufID, true)
	// wal log
	m.bm.ReleaseBuffer(bufID)

	// mark buffer dirty
	m.bm.MarkDirty(newbufID)

	m.bm.ReleaseContentLock(newbufID, true)
	// wal log
	m.bm.ReleaseBuffer(newbufID)
	return tmres, nil
}
