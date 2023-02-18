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
	"github.com/HayatoShiba/ppdb/transaction/clog"
)

// Manager is snapshot manager
type Manager struct {
	// clog manager is included
	// because, for checking visibility of the tuple, the transaction status has to be checked
	cm clog.Manager
}

// NewManager initializes snapshot manager
func NewManager(cm clog.Manager) *Manager {
	return &Manager{cm: cm}
}
