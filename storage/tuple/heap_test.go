package tuple

import (
	"reflect"
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestSetXmin(t *testing.T) {
	data := []byte{1, 2, 3}
	ctid := NewTid(page.FirstPageID, page.FirstSlotIndex)
	xmin := txid.FirstTxID
	tup := newTuple(xmin, ctid, xminCommitted, data)
	btup := marshalTuple(tup)

	expected := txid.TxID(100)
	btup.SetXmin(expected)
	assert.Equal(t, expected, btup.Xmin())
}

func TestSetXmax(t *testing.T) {
	data := []byte{1, 2, 3}
	ctid := NewTid(page.FirstPageID, page.FirstSlotIndex)
	xmin := txid.FirstTxID
	tup := newTuple(xmin, ctid, xminCommitted, data)
	btup := marshalTuple(tup)

	expected := txid.TxID(100)
	btup.SetXmax(expected)

	assert.Equal(t, expected, btup.Xmax())
}

func TestSetCtid(t *testing.T) {
	data := []byte{1, 2, 3}
	ctid := NewTid(page.FirstPageID, page.FirstSlotIndex)
	xmin := txid.FirstTxID
	tup := newTuple(xmin, ctid, xminCommitted, data)
	btup := marshalTuple(tup)

	expected := NewTid(page.PageID(21), page.SlotIndex(9))
	btup.SetCtid(expected)

	assert.True(t, reflect.DeepEqual(expected, btup.Ctid()))
}

func TestSetXminCommitted(t *testing.T) {
	data := []byte{1, 2, 3}
	ctid := NewTid(page.FirstPageID, page.FirstSlotIndex)
	xmin := txid.FirstTxID
	tup := newTuple(xmin, ctid, xminFrozen, data)
	btup := marshalTuple(tup)

	got := btup.XminCommitted()
	assert.False(t, got)

	btup.SetXminCommitted()
	got = btup.XminCommitted()
	assert.True(t, got)
}
