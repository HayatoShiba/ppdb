package transaction

import "github.com/HayatoShiba/ppdb/transaction/txid"

// Tx is a transaction
type Tx struct {
	id    txid.TxID
	state State
}

// NewTransaction initializes transaction
func NewTransaction(id txid.TxID) *Tx {
	return &Tx{
		id:    id,
		state: StateInProgress,
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

// SetState sets transaction state
func (tx *Tx) SetState(state State) {
	tx.state = state
}
