/*
Transaction id manager manages transaction id.
MVCC (MultiVersion Concurrency Control) needs timestamp and transaction id is used as kind of timestamp.

transaction id

This is implemented as manager because transaction id is kind of shared resource.
The latest transaction id has to be maintained and lock has to be held when allocating the transaction id.

---
About the nature of transaction id

Transaction id is defined as unsigned 32 bits and this can overflow.
note: In postgres, there is another type `FullTransactionId“ whose higher 32bit is
epoch and lower 32bit is transaction id (epoch is incremented when transaction id is wrapped around).
see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/access/transam.h#L60-L68

Anyway, transaction id can overflow so the space of transaction id has to be treated as a kind of circle.
When two transaction ids are compared, the overflow has to be considered. see IsFollows() method.

----
About XidGenLock

postgres uses lock (called XidGenLock) and I'm not sure why it doesn't use simple spinlock
https://github.com/postgres/postgres/blob/97c61f70d1b97bdfd20dcb1f2b1be42862ec88c2/src/backend/access/transam/README#L272-L284
*/
package txid

import (
	"sync"
)

type Manager struct {
	// the lock for xid is called XidGenLock in postgres.
	// this lock has to be acquired before generation of new transaction id.
	sync.Mutex
	// nextTxID is the transaction id which is alloted next time
	nextTxID TxID
}

// NewManager initializes transaction id manager
func NewManager() *Manager {
	return &Manager{
		nextTxID: FirstTxID,
	}
}

// AllocateNewTxID allocates next transaction id and advances it
// When this function is called, the state of newly allocated transaction has to be in progress so
// the id has to be inserted into snapshot manager's xip. this has to be done before releasing lock.
// see https://github.com/postgres/postgres/blob/97c61f70d1b97bdfd20dcb1f2b1be42862ec88c2/src/backend/access/transam/README#L272-L284
// https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/backend/access/transam/varsup.c#L50
func (tm *Manager) AllocateNewTxID() TxID {
	tm.Lock()
	// allocate next transaction id
	txID := tm.nextTxID
	// advance nextTxID
	tm.nextTxID = advanceTxID(tm.nextTxID)
	// TODO: check transaction id to prevent transactionID's wraparound
	// for preventing transactionID's wraparound, the vacuum limit has to be checked here.
	// the space of transaction id is about 4 billion, and halfway, that is 2 billion, is used for the boundary of visibility control.
	// which means, if vacuum has not frozen old transactions before 2 billion of transaction ids newly allocated,
	// the system of transaction id won't work well
	// if the newly allocated transaction id exceeds the threshold, then start autovacuum
	// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/backend/access/transam/varsup.c#L83-L166
	// https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/backend/access/transam/varsup.c#L345
	tm.Unlock()
	return txID
}
