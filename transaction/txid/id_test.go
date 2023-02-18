package txid

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFollows(t *testing.T) {
	tests := []struct {
		name     string
		txID1    TxID
		txID2    TxID
		expected bool
	}{
		{
			name:     "txID1 follow txID2 without overflow",
			txID1:    TxID(200),
			txID2:    TxID(199),
			expected: true,
		},
		{
			name:     "txID2 follow txID1 without overflow",
			txID1:    TxID(200),
			txID2:    TxID(201),
			expected: false,
		},
		{
			name:     "txID1 follows txID2 for overflow",
			txID1:    TxID(4),
			txID2:    TxID(uint32(math.Pow(2, 31)) + 100),
			expected: true,
		},
		{
			name:     "txID2 follows txID1 for overflow",
			txID1:    TxID(uint32(math.Pow(2, 31)) + 100),
			txID2:    TxID(4),
			expected: false,
		},
		{
			name:     "(boundary value test) txID1 doesn't follow txID2 with overflow",
			txID1:    TxID(100),
			txID2:    TxID(uint32(math.Pow(2, 31)) + 100),
			expected: false,
		},
		{
			name:     "(boundary value test) txID1 follows txID2 with overflow",
			txID1:    TxID(99),
			txID2:    TxID(uint32(math.Pow(2, 31)) + 100),
			expected: true,
		},
		{
			name:     "(boundary value test) txID1 doesn't follow txID2 with overflow",
			txID1:    TxID(101),
			txID2:    TxID(uint32(math.Pow(2, 31)) + 100),
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.txID1.IsFollows(tt.txID2)
			assert.Equal(t, tt.expected, got)
		})
	}
}
