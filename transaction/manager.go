/*
Postgres adopts MVCC(Multi Version Concurrency Control) for concurrency control.
MVCC is like `multiple version of tuple exists` so transaction can see the older version of tuple if necessary.
This can be achieved with timestamp ordering, which is transaction id in postgres.

In postgres, append-only storage approach is adopted to achieve MVCC.
So when the transaction updates the tuple, the new version of tuple is inserted (appended).
(the new version has the all fields of the tuple, including the field which is not updated)

And the version chain exists. The new version of tuple points to the old version of tuple in postgres through
the `ctid` field which every tuple has. The index points to the newest version, so when update the tuple, index
has to be updated (although this may be lazily executed later? TODO: research it)

Each tuple has the visibility information:
- xmin: what transaction inserts the tuple (begin time)
- xmax: what transaction updates/deletes the tuple (end time)
  - note:
  - update/delete query does not delete tuple physically. just insert end time to the tuple.
  - the tuples are linked through `ctid`

MVCC inserts new tuples / updates end time, but the transaction can be aborted finally.
so transaction status has to be persistent on disk and this is called `clog` in postgres.

The benefit of MVCC is `writers don't have to block readers / readers don't have to block writers`
But writers BLOCK writers.
so when the writer A tries to update/delete the tuple, and other writer B has done update/delete the same tuple,
and the writer B has not committed yet, then the writer A has to wait the writer B until the writer B
eventually commits or aborts the transaction.
when the writer B aborts the transaction, then the writer A can
continue the execution (probably). (TODO: see heap update function for understanding the behavior)
*/
package transaction

import (
	"github.com/HayatoShiba/ppdb/transaction/clog"
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
	"github.com/HayatoShiba/ppdb/transaction/txid"
)

type Manager struct {
	// if it isn't necessary to be exported, fix this later.
	Tm *txid.Manager
	Cm clog.Manager
	Sm *snapshot.Manager
}

func NewManager(tm *txid.Manager, cm clog.Manager, sm *snapshot.Manager) *Manager {
	return &Manager{
		Tm: tm,
		Cm: cm,
		Sm: sm,
	}
}

// Begin begins transaction
// see https://github.com/postgres/postgres/blob/20432f8731404d2cef2a155144aca5ab3ae98e95/src/backend/access/transam/xact.c#L2925
func (m *Manager) Begin() *Tx {
	// allocate new transaction id
	txID := m.Tm.AllocateNewTxID()
	// insert the txid into in progress txids for snapshot isolation
	m.Sm.AddInProgressTxID(txID)
	// after insertion of xip, lock can be released
	m.Tm.ReleaseLock()

	// TODO: this has to be executed in each statement in transaction, not in BEGIN.
	// TODO: enable to pass isolation level to Begin(). currently READ COMMITTED is fixed.
	level := defaultIsolationlevel
	if isIsolationUsesSameSnapshot(level) {
		_, ok := m.Sm.GetInProgressTxSnapshot(txID)
		if !ok {
			// this is the first snapshot after the transaction starts
			// take snapshot for the transaction
			snap := m.Sm.TakeSnapshot()
			// store txid and snapshot for vacuum
			m.Sm.AddInProgressTxSnapshot(txID, *snap)
		}
	} else {
		// take snapshot for the transaction
		snap := m.Sm.TakeSnapshot()
		// store txid and snapshot for vacuum
		m.Sm.AddInProgressTxSnapshot(txID, *snap)
	}

	return NewTransaction(txID, level)
}

// Commit commits transaction
func (m *Manager) Commit(tx Tx) {
	// store transaction state to clog
	m.Cm.SetStateCommitted(tx.ID())
	// remove the txid from in progress txids for snapshot isolation
	m.Sm.CompleteTxID(tx.ID())

	m.Sm.CompleteTxSnapshot(tx.ID())

	tx.SetState(StateCommitted)
}

// Abort aborts transaction
func (m *Manager) Abort(tx Tx) {
	// store transaction state to clog
	m.Cm.SetStateAborted(tx.ID())
	// remove the txid from in progress txids for snapshot isolation
	m.Sm.CompleteTxID(tx.ID())

	m.Sm.CompleteTxSnapshot(tx.ID())

	tx.SetState(StateAborted)
}
