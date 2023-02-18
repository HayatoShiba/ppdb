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
