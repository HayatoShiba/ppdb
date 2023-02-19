package tuple

import (
	"github.com/HayatoShiba/ppdb/transaction/txid"
)

func TestingNewTuple(xmin, xmax txid.TxID) TupleByte {
	data := []byte{1, 2, 3}
	btup := NewTuple(xmin, data)
	btup.SetXmax(xmax)
	return btup
}
