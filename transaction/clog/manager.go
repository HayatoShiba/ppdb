/*
Clog manager manages clog.
Clog is stored under pg_xact directory. In ppdb, only one file exists for clog under pg_xact directory.

----
About clog

Clog stores all transaction status,
and the reason why transaction status is necessary is described at comment at /storage/buffer/manager.go.
Simply put, the visibility of tuples cannot be determined without clog.

----
About clog buffer manager

The cache eviction policy is LRU(least-recently-used) while shared buffer uses clock-sweep algorithm.
The access pattern of clog is predictable to some extent.

- write operation is mostly to the latest clog page
  - so the latest page should not be evicted.

- read operation is basically to the small number of pages

see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L3

----
About clog interface

- check the transaction status, whether the transaction has been committed or aborted
- write the transaction status to clog file when the transaction is committed/aborted

----
About Vacuum

TODO: when vacuum, clog segments are truncated.
https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L878

see https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L3
*/
package clog

import (
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/pkg/errors"
)

// Manager is clog manager
type Manager struct {
	*bufferManager
}

// NewManager initializes manager
func NewManager() (*Manager, error) {
	dm, err := newDiskManager()
	if err != nil {
		return nil, errors.Wrap(err, "newDiskManager failed")
	}
	bm := newBufferManager(dm)
	return &Manager{bm}, nil
}

// IsTxCommitted checks whether the transaction has been committed or not
func (ma *Manager) IsTxCommitted(txID txid.TxID) (bool, error) {
	state, err := ma.getState(txID)
	if err != nil {
		return false, errors.Wrap(err, "getState failed")
	}
	return state == stateCommitted, nil
}

// IsTxAborted checks whether the transaction has been aborted or not
func (ma *Manager) IsTxAborted(id txid.TxID) (bool, error) {
	state, err := ma.getState(id)
	if err != nil {
		return false, errors.Wrap(err, "getState failed")
	}
	return state == stateAborted, nil
}

// SetStateCommitted sets transaction state `committed` to the corresponding location in bitmap
// https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L162
func (ma *Manager) SetStateCommitted(txID txid.TxID) error {
	if err := ma.updateState(txID, stateCommitted); err != nil {
		return errors.Wrap(err, "updateState failed")
	}
	return nil
}

// SetStateAborted sets transaction status `aborted` to the corresponding location in bitmap
// https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L162
func (ma *Manager) SetStateAborted(txID txid.TxID) error {
	if err := ma.updateState(txID, stateAborted); err != nil {
		return errors.Wrap(err, "updateState failed")
	}
	return nil
}
