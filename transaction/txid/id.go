package txid

// TxID is transaction id
// this can overflow
type TxID uint32

// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/access/transam.h#L31-L35
const (
	// invalid transaction id
	InvalidTxID TxID = 0
	// transaction id frozen by vacuum. (this is visible to any other transactions.)
	// frozen transaction id must be smaller than first transaction id
	FrozenTxID TxID = 2
	// first transaction id allocated by transaction id manager
	FirstTxID TxID = 3
)

// isNormal checks whether the transaction is normal
func (id TxID) isNormal() bool {
	return id >= FirstTxID
}

// IsEqual checks whether the transaction is equal to the compared
func (id TxID) IsEqual(compared TxID) bool {
	return id == compared
}

// IsFollows checks whether txID follows compared (txID >= compared)
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/access/transam.h#L332-L356
func (id TxID) IsFollows(compared TxID) bool {
	if !id.isNormal() || !compared.isNormal() {
		return id >= compared
	}
	diff := id - compared
	// if the diff is bigger than 2^31,
	// then the bigger is treaded as the older one because of conversion to int32.
	return int32(diff) > 0
}

// advanceTxID advances transaction id
// this considers wraparound of transaction id.
func advanceTxID(txID TxID) TxID {
	txID++
	if !txID.isNormal() {
		return FirstTxID
	}
	return txID
}
