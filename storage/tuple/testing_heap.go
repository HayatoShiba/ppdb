package tuple

import (
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/transaction/txid"
)

func TestingNewTuple(xmin, xmax txid.TxID) TupleByte {
	data := []byte{1, 2, 3}
	ctid := NewTid(page.FirstPageID, page.FirstSlotIndex)
	tup := newTuple(xmin, ctid, xminFrozen, data)
	btup := marshalTuple(tup)
	btup.SetXmax(xmax)
	return btup
}
