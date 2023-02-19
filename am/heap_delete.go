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

// TupleLocation is the location of tuple. information necessary for deleting heap tuple
type TupleLocation struct {
	// rel indicates what relation(table) the tuple belongs to
	rel common.Relation
	// tid is tuple's tid
	tid tuple.Tid
}

func NewTupleLocation(rel common.Relation, tid tuple.Tid) TupleLocation {
	return TupleLocation{
		rel: rel,
		tid: tid,
	}
}

/*
the logic to delete tuple (based on the assumption that the tuple has been already identified with tid)
- fetch the tuple
- check visibility and identify whether the tuple can be deleted
- if it cannot be deleted
  - if the tuple is invisible: return error
  - if other transaction is also updating the tuple and the transaction hasn't been committed
  - wait for the transaction to be committed/aborted （リピータブルリードだとエラーが出るはず？commit完了したら）

- if it can be deleted
  - set xmax with the current transaction id
  - set page flag to not used?
  - update visibility map. clear `all visible` and `all frozen`? if it is set.
    -

HeapDelete deletes tuple whose tid is specified in argument
`delete` means `set xmax` to the tuple so this tuple becomes invisible to the later transactions.
this function returns the tuple status result (updated/deleted by other transaction, successfully deleted by this transaction...)
this result will be used for the transaction behavior.
https://github.com/postgres/postgres/blob/63c844a0a5d70cdbd6ae0470d582d39e75ad8d66/src/backend/access/heap/heapam.c#L2670
*/
func (m *Manager) HeapDelete(tloc TupleLocation, tx transaction.Tx) (snapshot.TMResult, error) {
	// read buffer. the buffer has been pinned so this buffer cannot be evicted
	bufID, err := m.bm.ReadBuffer(tloc.rel, disk.ForkNumberMain, tloc.tid.PageID())
	if err != nil {
		return snapshot.TMResultInvisible, errors.Wrap(err, "ReadBuffer failed")
	}
	// acquire exclusive content lock for buffer to change
	m.bm.AcquireContentLock(bufID, true)

	p := m.bm.GetPage(bufID)
	item, err := page.GetItem(p, tloc.tid.SlotIndex())
	if err != nil {
		return snapshot.TMResultInvisible, errors.Wrap(err, "page.GetItem failed")
	}
	// convert item into tuple type
	tup := tuple.TupleByte(item)

	tmres, err := m.sm.CanTupleBeModified(tup, tloc.tid)
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

	// transaction isolation level is considered in another function
	// https://github.com/postgres/postgres/blob/4b3e37993254ed098219e62ceffb1b32fac388cb/src/backend/executor/nodeModifyTable.c#L1444-L1590
	// for example, if the transaction uses Repeatable Read, then when the tuple is finally updated/deleted by other concurrent transaction,
	// then it gets error `could not serialize access due to concurrent xxx`

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
		m.bm.UpdateVMStatus(tloc.rel, tloc.tid.PageID(), vm.StatusInitialized)
	}

	// postgres calculates xmax below
	// https://github.com/postgres/postgres/blob/63c844a0a5d70cdbd6ae0470d582d39e75ad8d66/src/backend/access/heap/heapam.c#L2926
	// ppdb simply set current transaction id
	tup.SetXmax(tx.ID())
	// mark buffer dirty
	m.bm.MarkDirty(bufID)

	m.bm.ReleaseContentLock(bufID, true)
	// wal log
	m.bm.ReleaseBuffer(bufID)

	return snapshot.TMResultOK, nil
}
