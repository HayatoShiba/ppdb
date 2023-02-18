package transaction

// Tx is a transaction
type Tx struct {
	state State
}

// NewTransaction initializes transaction
func NewTransaction() *Tx {
	return &Tx{
		state: StateInProgress,
	}
}

// State returns transaction state
func (tx *Tx) State() State {
	return tx.state
}

// SetState sets transaction state
func (tx *Tx) SetState(state State) {
	tx.state = state
}
