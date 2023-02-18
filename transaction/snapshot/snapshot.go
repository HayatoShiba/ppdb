package snapshot

import "github.com/HayatoShiba/ppdb/transaction/txid"

// Snapshot is snapshot
// see https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/utils/snapshot.h#L121
type Snapshot struct {
	// the minimum transaction id which is in progress
	// the number below xmin is expected to be completed
	xmin txid.TxID

	// the max transaction id which is completed
	// the number above xmax is expected to be invisible
	xmax txid.TxID

	// the transaction ids which must be in progress
	// allocation of new transaction id and insertion of the id to xip have to be atomic.
	// if those operations are not atomic, then the case below can happen.
	// - allocate transaction id 100
	// - allocate transaction id 101
	// - complete transaction id 101 and commit it
	// - the transaction id 101 becomes latestCompletedID(xmax). here, the transaction id 100 is not in xip, so
	// the transaction is considered `completed` afterwards. this leads to wrong behavior.
	xip map[txid.TxID]struct{}

	// TODO: add command id
	// https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/utils/snapshot.h#L187
}

// newSnapshot initializes snapshot
func newSnapshot(xmin, xmax txid.TxID, xip map[txid.TxID]struct{}) *Snapshot {
	return &Snapshot{
		xmin: xmin,
		xmax: xmax,
		xip:  xip,
	}
}

// isInProgress checks whether transaction id is in progress from perspective of this snapshot
// https://github.com/postgres/postgres/blob/8b5262fa0efdd515a05e533c2a1198e7b666f7d8/src/backend/utils/time/snapmgr.c#L2287
func (snap *Snapshot) isInProgress(txID txid.TxID) bool {
	// if txID < snap.xmin, then txID has been completed(committed/aborted)
	if snap.xmin.IsFollows(txID) {
		return false
	}
	// if txID > snap.xmax, then txID has not been completed from the snapshot's perspective
	if txID.IsFollows(snap.xmax) {
		return true
	}
	// here, snap.xmin <= txID <= snap.xmax
	// if the transaction id is in xip (txID in progress), it is in progress
	// maybe txID should be searched more efficiently, like with binary search
	for id, _ := range snap.xip {
		if id == txID {
			return true
		}
	}
	return false
}
