package snapshot

import (
	"github.com/HayatoShiba/ppdb/transaction/clog"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/pkg/errors"
)

func TestingNewManager(xip []txid.TxID, lcTxID txid.TxID) (*Manager, error) {
	cm, err := clog.TestingNewMockManager()
	if err != nil {
		return nil, errors.Wrap(err, "clog.NewManager failed")
	}
	m := NewManager(cm)
	for _, txID := range xip {
		m.AddInProgressTxID(txID)
	}
	m.latestCompletedTxID = lcTxID
	return m, nil
}
