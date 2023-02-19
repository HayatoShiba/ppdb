package am

import (
	"github.com/HayatoShiba/ppdb/storage/buffer"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/fsm"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/pkg/errors"
)

// TestingNewManager initializes the access method manager
func TestingNewManager() (*Manager, error) {
	dm, err := disk.TestingNewBufferManager()
	if err != nil {
		return nil, errors.Wrap(err, "disk.TestingNewBufferManager failed")
	}

	xip := []txid.TxID{20, 21, 40}
	var lcTxID txid.TxID = 30
	sm, err := snapshot.TestingNewManager(xip, lcTxID)
	if err != nil {
		return nil, errors.Wrap(err, "snapshot.TestingNewManager failed")
	}
	fm := fsm.TestingNewMockManager(page.FirstPageID, false)

	// TODO: mock buffer manager
	return NewManager(dm, buffer.NewManager(dm), sm, fm), nil
}
