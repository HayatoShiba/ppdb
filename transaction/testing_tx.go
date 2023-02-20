package transaction

import (
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
	"github.com/HayatoShiba/ppdb/transaction/txid"
)

func TestingNewTransaction(id, snapXmin, snapXmax txid.TxID, xips []txid.TxID) *Tx {
	sxip := make(map[txid.TxID]struct{})
	for _, txid := range xips {
		sxip[txid] = struct{}{}
	}
	snap := snapshot.NewSnapshot(snapXmin, snapXmax, sxip)
	return NewTransaction(id, defaultIsolationlevel, *snap)
}
