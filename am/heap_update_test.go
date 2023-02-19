package am

import (
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/transaction"
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestHeapUpdate(t *testing.T) {
	t.Run("successfully updated", func(t *testing.T) {
		am, err := TestingNewManager()
		assert.Nil(t, err)

		// the tuple to be deleted
		xmin1 := txid.TxID(10)
		xmax1 := txid.InvalidTxID
		tup1 := tuple.TestingNewTuple(xmin1, xmax1)

		rel := common.Relation(1)
		pageID := page.FirstPageID
		slotIndex := page.SlotIndex(1)
		data := []byte{1, 2, 3}
		huinfo := NewHeapUpdateInfo(rel, tuple.NewTid(pageID, slotIndex), data)

		// insert tuple manually
		// it may be better to mock buffer manager
		bufID, err := am.bm.ReadBuffer(rel, disk.ForkNumberMain, pageID)
		assert.Nil(t, err)
		p := am.bm.GetPage(bufID)
		page.InitializePage(p, 10)
		err = page.AddItem(p, page.ItemPtr(tup1), slotIndex)
		assert.Nil(t, err)

		xid := txid.TxID(15)
		snapXmin := txid.TxID(13)
		snapXmax := txid.TxID(14)
		tx := transaction.TestingNewTransaction(xid, snapXmin, snapXmax, []txid.TxID{xid})
		res, err := am.HeapUpdate(huinfo, *tx)
		assert.Nil(t, err)
		assert.Equal(t, snapshot.TMResultOK, res)

		// check tuple is deleted with xmax
		bufID, err = am.bm.ReadBuffer(rel, disk.ForkNumberMain, pageID)
		assert.Nil(t, err)
		p = am.bm.GetPage(bufID)
		item, err := page.GetItem(p, slotIndex)
		assert.Nil(t, err)
		tup := tuple.TupleByte(item)
		assert.Equal(t, xid, tup.Xmax())

		// get tuple's ctid to find the new version
		ctid := tup.Ctid()

		// re-fetch the page and check the tuple existence
		bufID, err = am.bm.ReadBuffer(rel, disk.ForkNumberMain, ctid.PageID())
		assert.Nil(t, err)
		p = am.bm.GetPage(bufID)
		item, err = page.GetItem(p, ctid.SlotIndex())
		assert.Nil(t, err)
		tup = tuple.TupleByte(item)
		assert.Equal(t, int(xid), int(tup.Xmin()))

	})
	t.Run("tuple invisible", func(t *testing.T) {
		am, err := TestingNewManager()
		assert.Nil(t, err)

		// the tuple invisible
		xmin2 := txid.InvalidTxID
		xmax2 := txid.InvalidTxID
		tup2 := tuple.TestingNewTuple(xmin2, xmax2)
		tup2.SetXminInvalid()

		rel := common.Relation(1)
		pageID := page.FirstPageID
		slotIndex := page.SlotIndex(1)
		data := []byte{1, 2, 3}
		huinfo := NewHeapUpdateInfo(rel, tuple.NewTid(pageID, slotIndex), data)

		// insert tuple manually
		// it may be better to mock buffer manager
		bufID, err := am.bm.ReadBuffer(rel, disk.ForkNumberMain, pageID)
		assert.Nil(t, err)
		p := am.bm.GetPage(bufID)
		page.InitializePage(p, 10)
		err = page.AddItem(p, page.ItemPtr(tup2), slotIndex)
		assert.Nil(t, err)

		xid := txid.TxID(15)
		snapXmin := txid.TxID(13)
		snapXmax := txid.TxID(14)
		tx := transaction.TestingNewTransaction(xid, snapXmin, snapXmax, []txid.TxID{xid})
		res, err := am.HeapUpdate(huinfo, *tx)
		assert.Nil(t, err)
		assert.Equal(t, snapshot.TMResultInvisible, res)

		// check tuple is not deleted with xmax
		bufID, err = am.bm.ReadBuffer(rel, disk.ForkNumberMain, pageID)
		assert.Nil(t, err)
		p = am.bm.GetPage(bufID)
		item, err := page.GetItem(p, slotIndex)
		assert.Nil(t, err)
		tup := tuple.TupleByte(item)
		assert.Equal(t, xmax2, tup.Xmax())
	})
}
