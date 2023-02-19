/*
Postgres implements snapshot isolation.
Snapshot is used for MVCC and this is deeply related with transaction isolation level.
Snapshot stores the information about the status of all transactions when the snapshot is taken.
With snapshot, transaction can determine whether the tuple is visible/updatable or not.

----
About snapshot

Snapshot stores transaction ids whose status is in progress when the snapshot is taken.
With this functionality, the transaction can avoid seeing the tuple that hasn't been inserted when the transaction began.

----
About transaction isolation level

Postgres determines when snapshot is taken for each transaction isolation level.
  - Repeatable Read: snapshot is taken when transaction starts(actually the first statement is executed?maybe), and
    the same snapshot is used during transaction.
  - Read Committed: snapshot is taken when each query statement is executed.

see for more detail: https://github.com/postgres/postgres/blob/20432f8731404d2cef2a155144aca5ab3ae98e95/src/include/access/xact.h#L33-L52

(under construction)
non-repeatable read can happen : Read Committed because snapshot is taken every statement
Repeatable Read cannot happen
phantom read cannot happen to Repeatable Read
when the first updater finally commits, then the transaction should be roll backed if it is repeatable read
if read committed, then the transaction can continue
see https://www.postgresql.org/docs/current/transaction-iso.html

---
About visibility

With snapshot, transaction can identify whether
another transaction which inserted the tuple is `in progress` or `already completed`.
But, when it is already completed, it cannot identify whether the transaction has been committed or aborted.
So clog has to be checked to determine the commit status of the transaction, then the visibility can be determined.
Additionally in postgres, after checking the commit status with clog, it sets hint bits of transaction status to
the tuple (probably so that clog doesn't have to be fetched afterwards. probably..)

---
Others

the explanation of snapshot type
https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/utils/snapshot.h#L23-L119

the visibility-related function is defined at heapam_visibility.c in postgres
https://github.com/postgres/postgres/blob/c3652cd84ac8aa60dd09a9743d4db6f20e985a2f/src/backend/access/heap/heapam_visibility.c#L3

the effect of snapshot for MVCC is described below
https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/utils/snapshot.h#L37-L50

the interface for checking visibility of tuple is defined as heap access method
https://github.com/postgres/postgres/blob/8e1db29cdbbd218ab6ba53eea56624553c3bef8c/src/backend/access/heap/heapam_handler.c#L2565

the snapshot is taken with GetSnapshotData() function
https://github.com/postgres/postgres/blob/8242752f9c104030085cb167e6e1dd5bed481360/src/backend/storage/ipc/procarray.c#L2214
*/
package snapshot

import (
	"sync"

	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/transaction/clog"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/pkg/errors"
)

// Manager is snapshot manager
type Manager struct {
	// clog manager is included
	// because, for checking visibility of the tuple, the transaction status has to be checked
	cm clog.Manager

	// this is called ProcArrayLock in postgres.
	// in ppdb, this lock is used for inProgressTxIDs and latestCompletedTxID
	mu sync.RWMutex

	// in progress transaction ids. this is used for snapshot isolation
	// in ppdb, this is implemented with hash simply.
	// https://github.com/postgres/postgres/blob/a4adc31f6902f6fc29d74868e8969412fc590da9/src/include/storage/proc.h#L370-L371
	inProgressTxIDs map[txid.TxID]struct{}

	// snapshots  whose transactions are in progress
	// vacuum has to know the oldest non removable transaction id
	// with inProgressSnapshots, we can know the oldest xmin
	// (transactions under the xmin are removable if they are dead)
	// TODO: maybe, we can persist just oldest xmin, not snapshot map.
	inProgressSnapshots map[txid.TxID]Snapshot

	// latest completed txid. this is used for snapshot isolation
	// this is used as xmax in snapshot.
	latestCompletedTxID txid.TxID
}

// NewManager initializes snapshot manager
func NewManager(cm clog.Manager) *Manager {
	return &Manager{
		cm:                  cm,
		inProgressTxIDs:     make(map[txid.TxID]struct{}),
		latestCompletedTxID: txid.InvalidTxID,
	}
}

// TakeSnapshot takes snapshot for transaction isolation
// see https://github.com/postgres/postgres/blob/8b5262fa0efdd515a05e533c2a1198e7b666f7d8/src/backend/utils/time/snapmgr.c#L241-L318
func (m *Manager) TakeSnapshot() *Snapshot {
	xmax, xmin := m.getSnapshotInfo()
	return &Snapshot{
		xmin: xmin,
		xmax: xmax,
		xip:  m.inProgressTxIDs,
	}
}

// GetSnapshotInfo returns xmax and xmin
func (m *Manager) getSnapshotInfo() (xmin, xmax txid.TxID) {
	// shared lock has to be held when latestCompletedTxID and inProgressTxIDs are used.
	// deleting from inProgressTxIDs and inserting latestCompletedTxID (if necessary) must be atomic for consistency.
	// see https://github.com/postgres/postgres/blob/97c61f70d1b97bdfd20dcb1f2b1be42862ec88c2/src/backend/access/transam/README#L246-L257
	m.mu.RLock()
	defer m.mu.RUnlock()

	// latestCompletedTxID is initialized with txid.InvalidTxID
	// it means that all transactions are invisible
	xmax = m.latestCompletedTxID

	// xmin is calculated from in-progress xids
	// current txid is expected to be in xip if the txid is in progress
	// so ppdb assumes that, at least, one xid exists in xip slice when take snapshot
	xmin = txid.InvalidTxID
	for id, _ := range m.inProgressTxIDs {
		if xmin == txid.InvalidTxID {
			xmin = id
		}
		if xmin.IsFollows(id) {
			xmin = id
		}
	}
	return
}

// AddInProgressTxID adds the txid to inProgressTxIDs
// this is expected to be called when transaction id is newly allocated.
// the allocated ids has to be stored to inProgressTxIDs.
// the caller has to hold XidGenLock.
func (m *Manager) AddInProgressTxID(txID txid.TxID) {
	m.inProgressTxIDs[txID] = struct{}{}
}

// IsInProgressTxID checks whether the transaction is in progress in terms of the system (not snapshot)
func (m *Manager) IsInProgressTxID(txID txid.TxID) bool {
	_, ok := m.inProgressTxIDs[txID]
	return ok
}

// CompleteTxID removes the txid from in progress txids.
// if the txid is latest completed id, then update the field.
func (m *Manager) CompleteTxID(txID txid.TxID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inProgressTxIDs, txID)
	if txID.IsFollows(m.latestCompletedTxID) {
		m.latestCompletedTxID = txID
	}
}

// AddInProgressTxSnapshot adds the txid and snapshot to inProgressTxIDs
// this is expected to be called when transaction id is newly allocated.
// the allocated ids has to be stored to inProgressTxIDs.
// the caller doesn't have to hold any lock. this can result in the case txid is stored only in
// inProgressTxIDs, but not inProgressSnapshots. this case is considered when vacuum.
func (m *Manager) AddInProgressTxSnapshot(txID txid.TxID, snap Snapshot) {
	m.inProgressSnapshots[txID] = snap
}

// GetInProgressTxSnapshot returns in progress tx snapshot
func (m *Manager) GetInProgressTxSnapshot(txID txid.TxID) (Snapshot, bool) {
	snap, ok := m.inProgressSnapshots[txID]
	return snap, ok
}

// CompleteTxSnapshot removes the snapshot from in progress snapshots.
func (m *Manager) CompleteTxSnapshot(txID txid.TxID) {
	delete(m.inProgressSnapshots, txID)
}

// isTupleVisibleFromSnapshot determines the visibility of the tuple from the snapshot
// This is expected to be called when get/update tuple for visibility check
// https://github.com/postgres/postgres/blob/c3652cd84ac8aa60dd09a9743d4db6f20e985a2f/src/backend/access/heap/heapam_visibility.c#L965
func (m *Manager) IsTupleVisibleFromSnapshot(tuple tuple.TupleByte, snap *Snapshot) (bool, error) {
	/*
		the logic to check visibility of the tuple is described below
		- check the transaction status of tuple's xmin from this snapshot
			- if tuple's xmin < snapshot's xmin, then xmin has been completed(committed/aborted)
			  - if xmin has been committed, then xmax has to be checked for visibility check
			  - if xmin has been aborted, then invisible to this snapshot
			- if tuple's xmin > snapshot's xmax, then invisible to this snapshot
			- if snasphot's xmin < tuple's xmin < snapshot's xmax, then check snasphot's xip
			- if tuple's xmin exists in xip, then invisible to this snapshot
			- if tuple's xmin doesn't exist in xip, then xmin has been completed(committed/aborted)
				- if xmin has been committed, then xmax has to be checked for visibility check
				- if xmin has been aborted, then invisible to this snapshot
		- if xmin has been committed from this snapshot, xmax has to be checked
			- if tuple's xmax is invalid, then the tuple is not updated/delete so VISIBLE TO THIS SNAPSHOT
			- if tuple's xmax < snapshot's xmin, then xmax has been completed(committed/aborted)
			  - if xmax has been committed, then INVISIBLE to this snapshot
			  - if xmax has been aborted, then VISIBLE to this snapshot
			- if tuple's xmax > snapshot's xmax, then VISIBLE to this snapshot
			- if snasphot's xmin < tuple's xmax < snapshot's xmax, then check snasphot's xip
			- if tuple's xmax exists in xip, then VISIBLE to this snapshot
			- if tuple's xmax doesn't exist in xip, then xmax has been completed(committed/aborted)
				- if xmax has been committed, then invisible to this snapshot
				- if xmax has been aborted, then VISIBLE to this snapshot
	*/

	if !tuple.XminCommitted() {
		if tuple.XminInvalid() {
			return false, nil
		}
		// if tuple's xmin > snapshot's xmax
		if tuple.Xmin().IsFollows(snap.xmax) {
			return false, nil
		}

		// check whether tuple's xmin is in progress
		if snap.isInProgress(tuple.Xmin()) {
			// if xmin is in progress, then the tuple is invisible
			return false, nil
		}
		// we know xmin has been completed here.
		// next, then transaction status has to be checked.
		aborted, err := m.cm.IsTxAborted(tuple.Xmin())
		if err != nil {
			return false, errors.Wrap(err, "m.cm.IsTxAborted failed")
		}
		if aborted {
			// if xmin's transaction has been aborted, the tuple is invisible
			return false, nil
		}

		// here, we know the xmin's transaction has been committed(not aborted),
		// postgres set transaction status hint bits for (probably) performance improvement of
		// checking status next time
		tuple.SetXminCommitted()
	}

	if !tuple.XmaxCommitted() {
		if tuple.XmaxInvalid() {
			return true, nil
		}
		// if tuple's xmax > snapshot's xmax
		if tuple.Xmax().IsFollows(snap.xmax) {
			return true, nil
		}

		// so we know xmin has been committed here, check xmax in the same way
		// if xmax is in progress, then the tuple is VISIBLE. because the tuple is not updated/deleted
		if snap.isInProgress(tuple.Xmax()) {
			return true, nil
		}
		aborted, err := m.cm.IsTxAborted(tuple.Xmax())
		if err != nil {
			return false, errors.Wrap(err, "m.cm.IsTxAborted failed")
		}
		if aborted {
			return true, nil
		}
		// here, xmax has been committed, so the tuple is invisible
		tuple.SetXmaxCommitted()
	}
	return false, nil
}

// IsTupleVacuumable checks whether the tuple can be vacuumed or not.
// to determine the visibility of tuple, this function checks:
// - the commit status of xmin,xmax of the tuple
// - the visibility of the tuple from oldest snapshot
// https://github.com/postgres/postgres/blob/c3652cd84ac8aa60dd09a9743d4db6f20e985a2f/src/backend/access/heap/heapam_visibility.c#L1161
func (m *Manager) IsTupleVacuumable(tuple tuple.TupleByte) (bool, error) {
	status, deleted, err := m.GetTupleVisibilityStatus(tuple)
	if err != nil {
		return false, errors.Wrap(err, "m.GetTupleVisibilityStatus failed")
	}
	// if the tuple has been deleted, we have to check the tuple is invisible from oldest snapshot
	if status == TupleVisibilityStatusRecentlyDead {
		// https://github.com/postgres/postgres/blob/8242752f9c104030085cb167e6e1dd5bed481360/src/backend/storage/ipc/procarray.c#L2013
		// TODO: this assignment and loop must be refactored. this is messy............
		oldestXmin := txid.InvalidTxID
		for id, _ := range m.inProgressTxIDs {
			if oldestXmin == txid.InvalidTxID {
				oldestXmin = id
			}
			for _, snapshot := range m.inProgressSnapshots {
				if oldestXmin.IsFollows(snapshot.xmin) {
					oldestXmin = snapshot.xmin
				}
			}
		}
		if oldestXmin.IsFollows(deleted) {
			// this can be vacuumed
			return true, nil
		}
	}
	return false, nil
}

// TupleVisibilityStatus is the status for determining the visibility of tuple
type TupleVisibilityStatus uint

const (
	// TupleVisibilityStatusInvalid indicates the tuple is invalid
	TupleVisibilityStatusInvalid TupleVisibilityStatus = iota
	// TupleVisibilityStatusRecentlyDead indicates the tuple has recently been deleted.
	// fot vacuum, the visibility (from oldest snapshot) must also be checked.
	TupleVisibilityStatusRecentlyDead
	// TupleVisibilityStatusDead indicates the tuple is dead and invisible to all transactions
	TupleVisibilityStatusDead
	// TupleVisibilityStatuseAlive indicates the tuole is alive and cannot be vacuumed.
	TupleVisibilityStatuseAlive
)

// IsTupleVacuumable determines whether the tuple can be vacuumed or not
// for the details of the logic, see comment at IsTupleVisibleFromSnapshot().
// it returns transaction id which deletes(updates) the tuple if the tuple is deleted
// when the tuple is not actually inserted because of abort, invalid transaction id is also returned.
// TODO: this is not actually related with snapshot so this function may be defined elsewhere
// https://github.com/postgres/postgres/blob/c3652cd84ac8aa60dd09a9743d4db6f20e985a2f/src/backend/access/heap/heapam_visibility.c#L1195
func (m *Manager) GetTupleVisibilityStatus(tuple tuple.TupleByte) (TupleVisibilityStatus, txid.TxID, error) {
	// deleted is transaction id which deletes(updates) the tuple (if the tuple has been deleted(updated))
	deleted := txid.InvalidTxID
	// we know xmin has been completed here.
	// next, then transaction status has to be checked.
	aborted, err := m.cm.IsTxAborted(tuple.Xmin())
	if err != nil {
		return TupleVisibilityStatusInvalid, deleted, errors.Wrap(err, "m.cm.IsTxAborted failed")
	}
	if aborted {
		// if xmin's transaction has been aborted, the tuple is invisible
		return TupleVisibilityStatusRecentlyDead, deleted, nil
	}

	// here, we know the xmin's transaction has been committed(not aborted),
	// postgres set transaction status hint bits for (probably) performance improvement of
	// checking status next time

	aborted, err = m.cm.IsTxAborted(tuple.Xmax())
	if err != nil {
		return TupleVisibilityStatusInvalid, deleted, errors.Wrap(err, "m.cm.IsTxAborted failed")
	}
	if aborted {
		return TupleVisibilityStatuseAlive, deleted, nil
	}
	// here, xmax has been committed, so the tuple is invisible
	deleted = tuple.Xmax()
	return TupleVisibilityStatusRecentlyDead, deleted, nil
}
