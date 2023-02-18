package clog

import (
	"testing"

	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestSetStateAborted(t *testing.T) {
	m, err := TestingNewManager(t)
	assert.Nil(t, err)

	tests := []struct {
		name string
		txID txid.TxID
	}{
		{
			name: "txID is 0",
			txID: 0,
		},
		{
			name: "txID is 100",
			txID: 100,
		},
		{
			name: "txID is 9000",
			txID: 9000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.IsTxAborted(tt.txID)
			assert.Nil(t, err)
			assert.False(t, got)

			err = m.SetStateAborted(tt.txID)
			assert.Nil(t, err)
			got, err = m.IsTxAborted(tt.txID)
			assert.Nil(t, err)
			assert.True(t, got)
		})
	}
}

func TestSetStateCommitted(t *testing.T) {
	m, err := TestingNewManager(t)
	assert.Nil(t, err)

	tests := []struct {
		name string
		txID txid.TxID
	}{
		{
			name: "txID is 0",
			txID: 0,
		},
		{
			name: "txID is 100",
			txID: 100,
		},
		{
			name: "txID is 9000",
			txID: 9000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.IsTxCommitted(tt.txID)
			assert.Nil(t, err)
			assert.False(t, got)

			err = m.SetStateCommitted(tt.txID)
			assert.Nil(t, err)
			got, err = m.IsTxCommitted(tt.txID)
			assert.Nil(t, err)
			assert.True(t, got)
		})
	}
}
