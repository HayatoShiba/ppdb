package snapshot

import (
	"testing"

	"github.com/HayatoShiba/ppdb/storage/tuple"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestGetSnapshotInfo(t *testing.T) {
	t.Run("no xip, no completed xid", func(t *testing.T) {
		m, err := TestingNewManager([]txid.TxID{10}, txid.InvalidTxID)
		assert.Nil(t, err)
		m.AddInProgressTxID(10)
		xmin, xmax := m.getSnapshotInfo()
		assert.Equal(t, txid.TxID(10), xmin)
		assert.Equal(t, txid.InvalidTxID, xmax)
	})
	t.Run("xip exists, and latestCompleteTxID is also stored", func(t *testing.T) {
		xip := []txid.TxID{20, 21}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin, xmax := m.getSnapshotInfo()
		assert.Equal(t, txid.TxID(20), xmin)
		assert.Equal(t, txid.TxID(30), xmax)
	})
}

func TestCompleteTxID(t *testing.T) {
	t.Run("when needs update on latestCompletedTxID", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		expected := txid.TxID(40)
		_, ok := m.inProgressTxIDs[expected]
		assert.True(t, ok)
		m.CompleteTxID(expected)
		_, ok = m.inProgressTxIDs[expected]
		assert.False(t, ok)
		assert.Equal(t, expected, m.latestCompletedTxID)
	})
	t.Run("when no update on latestCompletedTxID", func(t *testing.T) {
		xip := []txid.TxID{20, 21}
		var expected txid.TxID = 30
		m, err := TestingNewManager(xip, expected)
		assert.Nil(t, err)

		m.CompleteTxID(txid.TxID(21))
		assert.Equal(t, expected, m.latestCompletedTxID)
	})
}

func TestIsTupleVisibleFromSnapshot(t *testing.T) {
	t.Run("when tuple's xmin < snapshot's xmin and xmin is aborted", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		abortedXmin := txid.TxID(10)
		xmax := txid.TxID(20)
		m.cm.SetStateAborted(abortedXmin)

		sxip := make(map[txid.TxID]struct{})
		sxip[14] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(abortedXmin, xmax), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when tuple's xmin > snapshot's xmax", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(1000)
		xmax := txid.TxID(2000)

		sxip := make(map[txid.TxID]struct{})
		sxip[14] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when snasphot's xmin < tuple's xmin < snapshot's xmax, and tuple's xmin exists in xip", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(15)
		sxip := make(map[txid.TxID]struct{})
		sxip[15] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, txid.InvalidTxID), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when snasphot's xmin < tuple's xmin < snapshot's xmax, and tuple's xmin doesn't exist in xip, and xmin is aborted", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(16)
		m.cm.SetStateAborted(xmin)

		sxip := make(map[txid.TxID]struct{})
		sxip[15] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, txid.InvalidTxID), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when snasphot's xmin < tuple's xmin < snapshot's xmax, and tuple's xmin doesn't exist in xip, and xmin is committed and xmax is in progress", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(16)
		xmax := txid.TxID(17)
		m.cm.SetStateCommitted(xmin)

		sxip := make(map[txid.TxID]struct{})
		sxip[xmax] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.True(t, isVisible)
	})
	t.Run("when tuple's xmin has been committed, and xmax is invalid", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(10)
		m.cm.SetStateCommitted(xmin)

		sxip := make(map[txid.TxID]struct{})
		sxip[14] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, txid.InvalidTxID), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when tuple's xmin has been committed, and xmax < snapshot's xmin and xmax has been committed", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(10)
		xmax := txid.TxID(11)
		m.cm.SetStateCommitted(xmin)
		m.cm.SetStateCommitted(xmax)

		sxip := make(map[txid.TxID]struct{})
		sxip[14] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when tuple's xmin has been committed, and xmax < snapshot's xmin and xmax has been aborted", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(10)
		xmax := txid.TxID(11)
		m.cm.SetStateCommitted(xmin)
		m.cm.SetStateAborted(xmax)

		sxip := make(map[txid.TxID]struct{})
		sxip[14] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.True(t, isVisible)
	})
	t.Run("when tuple's xmin has been committed, and snasphot's xmin < tuple's xmax < snapshot's xmax, tuple's xmax exists in xip", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(10)
		xmax := txid.TxID(14)
		m.cm.SetStateCommitted(xmin)

		sxip := make(map[txid.TxID]struct{})
		sxip[xmax] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.True(t, isVisible)
	})
	t.Run("when tuple's xmin has been committed, and snasphot's xmin < tuple's xmax < snapshot's xmax, tuple's xmax  doesn't exist in xip, and has been committed", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(10)
		xmax := txid.TxID(14)
		m.cm.SetStateCommitted(xmin)
		m.cm.SetStateCommitted(xmax)

		sxip := make(map[txid.TxID]struct{})
		sxip[15] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.False(t, isVisible)
	})
	t.Run("when tuple's xmin has been committed, and snasphot's xmin < tuple's xmax < snapshot's xmax, tuple's xmax  doesn't exist in xip, and has been aborted", func(t *testing.T) {
		xip := []txid.TxID{20, 21, 40}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(xip, lcTxID)
		assert.Nil(t, err)

		xmin := txid.TxID(10)
		xmax := txid.TxID(14)
		m.cm.SetStateCommitted(xmin)
		m.cm.SetStateAborted(xmax)

		sxip := make(map[txid.TxID]struct{})
		sxip[15] = struct{}{}
		s := newSnapshot(txid.TxID(13), txid.TxID(18), sxip)

		isVisible, err := m.IsTupleVisibleFromSnapshot(tuple.TestingNewTuple(xmin, xmax), s)
		assert.Nil(t, err)
		assert.True(t, isVisible)
	})
}
