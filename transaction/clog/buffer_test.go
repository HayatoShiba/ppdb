package clog

import (
	"testing"

	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestGetStateUpdateState(t *testing.T) {
	bm, err := TestingNewBufferManager(t)
	assert.Nil(t, err)

	tests := []struct {
		name     string
		txID     txid.TxID
		expected state
	}{
		{
			name:     "txID is 0",
			txID:     0,
			expected: stateInProgress,
		},
		{
			name:     "txID is 100",
			txID:     100,
			expected: stateCommitted,
		},
		{
			name:     "txID is 9000",
			txID:     9000,
			expected: stateAborted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := bm.updateState(tt.txID, tt.expected)
			assert.Nil(t, err)
			got, err := bm.getState(tt.txID)
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}
