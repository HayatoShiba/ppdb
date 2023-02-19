package am

import (
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/transaction"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestGetBufferForInsert(t *testing.T) {
	am, err := TestingNewManager()
	assert.Nil(t, err)
	_, pageID, err := am.getBufferForInsert(common.Relation(0), 10)
	assert.Nil(t, err)
	assert.Equal(t, page.FirstPageID, pageID)
}

func TestInsertTuple(t *testing.T) {
	p, err := page.TestingNewRandomPage()
	assert.Nil(t, err)

	nidx := page.GetNSlotIndex(p)

	tup := tuple.TestingNewTuple(txid.TxID(10), txid.InvalidTxID)
	var pageID page.PageID = 10
	err = insertTuple(p, tup, pageID)
	assert.Nil(t, err)

	// get and check tuple from the page
	got, err := page.GetItem(p, nidx+1)
	assert.Nil(t, err)
	gotTuple := tuple.TupleByte(got)
	ctid := gotTuple.Ctid()
	assert.Equal(t, pageID, ctid.PageID())
	assert.Equal(t, nidx+1, ctid.SlotIndex())
}

func TestHeapInsert(t *testing.T) {
	am, err := TestingNewManager()
	assert.Nil(t, err)

	hinfo := NewHeapInsertInfo(common.Relation(0), []byte{1, 2, 3})
	xid := txid.TxID(15)
	snapXmin := txid.TxID(13)
	snapXmax := txid.TxID(14)
	tx := transaction.TestingNewTransaction(xid, snapXmin, snapXmax, []txid.TxID{xid})
	err = am.HeapInsert(hinfo, *tx)
	assert.Nil(t, err)

	// re-fetch the page and check the tuple existence
	bufID, err := am.bm.ReadBuffer(common.Relation(0), disk.ForkNumberMain, page.FirstPageID)
	assert.Nil(t, err)
	p := am.bm.GetPage(bufID)
	item, err := page.GetItem(p, page.FirstSlotIndex)
	tup := tuple.TupleByte(item)
	assert.Nil(t, err)
	assert.Equal(t, xid, tup.Xmin())
}
