package snapshot

import (
	"testing"

	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestIsInProgress(t *testing.T) {
	xip := make(map[txid.TxID]struct{})
	var xmin txid.TxID = 10
	var inProgressXid txid.TxID = 15
	xip[inProgressXid] = struct{}{}
	xip[xmin] = struct{}{}

	tests := []struct {
		name       string
		xmin       txid.TxID
		xmax       txid.TxID
		targetTxID txid.TxID
		expected   bool // is in progress
	}{
		{
			name:       "target is smaller than xmin",
			xmin:       xmin,
			xmax:       20,
			targetTxID: 9,
			expected:   false,
		},
		{
			name:       "target is the same as xmin",
			xmin:       xmin,
			xmax:       20,
			targetTxID: xmin,
			expected:   true,
		},
		{
			name:       "target is bigger than xmin",
			xmin:       xmin,
			xmax:       20,
			targetTxID: 11,
			expected:   false,
		},
		{
			name:       "target is smaller than xmax",
			xmin:       xmin,
			xmax:       20,
			targetTxID: 19,
			expected:   false,
		},
		{
			name:       "target is the same as xmax",
			xmin:       xmin,
			xmax:       20,
			targetTxID: 20,
			expected:   false,
		},
		{
			name:       "target is bigger than xmax",
			xmin:       xmin,
			xmax:       20,
			targetTxID: 21,
			expected:   true,
		},
		{
			name:       "target is bigger than xmin, smaller than xmax, and in xip",
			xmin:       xmin,
			xmax:       20,
			targetTxID: inProgressXid,
			expected:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := NewSnapshot(tt.xmin, tt.xmax, xip)
			got := snap.isInProgress(tt.targetTxID)
			assert.Equal(t, tt.expected, got)
		})
	}
}
