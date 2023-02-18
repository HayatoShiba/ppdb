package snapshot

import (
	"testing"

	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestGetSnapshotInfo(t *testing.T) {
	t.Run("no xip, no completed xid", func(t *testing.T) {
		m, err := TestingNewManager(t, []txid.TxID{10}, txid.InvalidTxID)
		assert.Nil(t, err)
		m.AddInProgressTxID(10)
		xmin, xmax := m.getSnapshotInfo()
		assert.Equal(t, txid.TxID(10), xmin)
		assert.Equal(t, txid.InvalidTxID, xmax)
	})
	t.Run("xip exists, and latestCompleteTxID is also stored", func(t *testing.T) {
		xip := []txid.TxID{20, 21}
		var lcTxID txid.TxID = 30
		m, err := TestingNewManager(t, xip, lcTxID)
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
		m, err := TestingNewManager(t, xip, lcTxID)
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
		m, err := TestingNewManager(t, xip, expected)
		assert.Nil(t, err)

		m.CompleteTxID(txid.TxID(21))
		assert.Equal(t, expected, m.latestCompletedTxID)
	})
}
