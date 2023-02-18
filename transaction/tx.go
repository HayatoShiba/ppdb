package transaction

import "github.com/HayatoShiba/ppdb/transaction/txid"

// Tx is a transaction
type Tx struct {
	id    txid.TxID
	state State
	level isolationLevel
}

// NewTransaction initializes transaction
func NewTransaction(id txid.TxID, level isolationLevel) *Tx {
	return &Tx{
		id:    id,
		state: StateInProgress,
		level: level,
	}
}

// ID returns transaction id
func (tx *Tx) ID() txid.TxID {
	return tx.id
}

// State returns transaction state
func (tx *Tx) State() State {
	return tx.state
}

// IsolationLevel returns transaction isolation level
func (tx *Tx) IsolationLevel() isolationLevel {
	return tx.level
}

// SetState sets transaction state
func (tx *Tx) SetState(state State) {
	tx.state = state
}
