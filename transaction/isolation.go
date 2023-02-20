/*
postgres defines transaction isolation level below
https://github.com/postgres/postgres/blob/20432f8731404d2cef2a155144aca5ab3ae98e95/src/include/access/xact.h#L33-L52
IsolationUsesXactSnapshot() is used, for example, when
  - transaction tries to update tuple. when another transaction concurrently updates the tuple,
    the result is different depending on the isolation level.
    (probably, repeatable read isolation level omits error, while read committed isolation level continues to process without error)
  - snapshot is taken. read committed isolation level takes snapshot per each statement in transaction, while
    repeatable read isolation level takes snapshot per transaction and uses the same snapshot during the transaction.
*/
package transaction

type isolationLevel uint

const (
	isolationLevelReadUncommitted isolationLevel = iota
	isolationLevelReadCommitted
	isolationLevelRepeatableRead
	isolationLevelSerializable

	// default isolation level is READ COMMITTED
	defaultIsolationlevel = isolationLevelReadCommitted
)

// isIsolationUsesSameSnapshot returns whether the isolation level uses the same snapshot during a transaction
func isIsolationUsesSameSnapshot(level isolationLevel) bool {
	return level >= isolationLevelRepeatableRead
}
