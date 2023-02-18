package transaction

// State is transaction state
// this is exported because probably mapping from tx state to state stored on clog file is necessary in clog package
// see https://github.com/postgres/postgres/blob/20432f8731404d2cef2a155144aca5ab3ae98e95/src/backend/access/transam/xact.c#L137-L148
type State uint

const (
	// during transaction
	StateInProgress = iota
	// transaction committed
	StateCommitted
	// transaction aborted
	StateAborted
)

// IsCompleted checks whether the transaction has been completed
func IsCompleted(state State) bool {
	if state == StateCommitted || state == StateAborted {
		return true
	}
	return false
}
