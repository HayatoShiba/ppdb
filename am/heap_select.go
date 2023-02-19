package am

import (
	"fmt"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/transaction"
	"github.com/pkg/errors"
)

type scanResults struct {
	tuples []tuple.TupleByte
}

// this is defined temporary. I haven't read the original function so much.
// https://github.com/postgres/postgres/blob/63c844a0a5d70cdbd6ae0470d582d39e75ad8d66/src/backend/access/heap/heapam.c#L1350
func (m *Manager) HeapSequentialScan(rel common.Relation, tx transaction.Tx) (scanResults, error) {
	res := scanResults{
		// 10 is temporary defined
		tuples: make([]tuple.TupleByte, 0, 10),
	}
	npid, err := m.dm.GetNPageID(rel, disk.ForkNumberMain)
	if err != nil {
		return res, errors.Wrap(err, "GetNPageID failed")
	}
	for pageID := page.FirstPageID; pageID <= npid; pageID++ {
		bufID, err := m.bm.ReadBuffer(rel, disk.ForkNumberMain, pageID)
		if err != nil {
			return res, errors.Wrap(err, "ReadBuffer failed")
		}
		// acquire shared content lock
		m.bm.AcquireContentLock(bufID, false)
		p := m.bm.GetPage(bufID)
		nslotIndex := page.GetNSlotIndex(p)
		if nslotIndex == page.InvalidSlotIndex {
			continue
		}
		for slotIndex := page.FirstSlotIndex; slotIndex <= nslotIndex; slotIndex++ {
			item, err := page.GetItem(p, slotIndex)
			if err != nil {
				return res, errors.Wrap(err, "GetItem failed")
			}
			tup := tuple.TupleByte(item)
			// check visibility
			snap := tx.Snapshot()
			visible, err := m.sm.IsTupleVisibleFromSnapshot(tup, &snap)
			if err != nil {
				return res, errors.Wrap(err, "IsTupleVisibleFromSnapshot failed")
			}
			fmt.Println(slotIndex)
			fmt.Println(tup)
			fmt.Println(visible)
			if visible {
				res.tuples = append(res.tuples, tup)
			}
		}
	}
	return res, nil
}
