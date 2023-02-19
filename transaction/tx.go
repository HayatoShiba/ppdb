package transaction

import (
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
	"github.com/HayatoShiba/ppdb/transaction/txid"
)

// Tx is a transaction
type Tx struct {
	id       txid.TxID
	state    State
	level    isolationLevel
	snapshot snapshot.Snapshot
}

// NewTransaction initializes transaction
func NewTransaction(id txid.TxID, level isolationLevel, snapshot snapshot.Snapshot) *Tx {
	return &Tx{
		id:       id,
		state:    StateInProgress,
		level:    level,
		snapshot: snapshot,
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

// Snapshot returns snapshot
func (tx *Tx) Snapshot() snapshot.Snapshot {
	return tx.snapshot
}
