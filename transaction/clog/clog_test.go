package clog

import (
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/transaction/txid"
	"github.com/stretchr/testify/assert"
)

func TestGetPageIDFromTxID(t *testing.T) {
	tests := []struct {
		name   string
		txID   txid.TxID
		pageID page.PageID
	}{
		{
			name:   "txID is 0",
			txID:   0,
			pageID: 0,
		},
		{
			name:   "txID is clogNumPerPage-1",
			txID:   clogNumPerPage - 1,
			pageID: 0,
		},
		{
			name:   "txID is clogNumPerPage",
			txID:   clogNumPerPage,
			pageID: 1,
		},
		{
			name:   "txID is clogNumPerPage+1",
			txID:   clogNumPerPage + 1,
			pageID: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getPageIDFromTxID(tt.txID)
			assert.Equal(t, tt.pageID, got)
		})
	}
}

func TestGetByteOffsetFromTxID(t *testing.T) {
	tests := []struct {
		name       string
		txID       txid.TxID
		byteOffset int
	}{
		{
			name:       "txID is 0",
			txID:       0,
			byteOffset: 0,
		},
		{
			name:       "txID is 1",
			txID:       1,
			byteOffset: 0,
		},
		{
			name:       "txID is clogNumPerPage-1",
			txID:       clogNumPerPage - 1,
			byteOffset: page.PageSize - 1,
		},
		{
			name:       "txID is clogNumPerPage",
			txID:       clogNumPerPage,
			byteOffset: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getByteOffsetFromTxID(tt.txID)
			assert.Equal(t, tt.byteOffset, got)
		})
	}
}

func TestGetBitOffsetFromTxID(t *testing.T) {
	tests := []struct {
		name   string
		txID   txid.TxID
		offset int
	}{
		{
			name:   "txID is 0",
			txID:   0,
			offset: 0,
		},
		{
			name:   "txID is 1",
			txID:   1,
			offset: 2,
		},
		{
			name:   "txID is 4",
			txID:   4,
			offset: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getBitOffsetFromTxID(tt.txID)
			assert.Equal(t, tt.offset, got)
		})
	}
}

func TestGetState(t *testing.T) {
	tests := []struct {
		name     string
		txID     txid.TxID
		data     byte
		expected state
	}{
		// byte offset is 0
		{
			name:     "txID is 0: state is in progress",
			txID:     0,
			data:     byte(0b00000000),
			expected: stateInProgress,
		},
		{
			name:     "txID is 0: state is committed",
			txID:     0,
			data:     byte(0b01000000),
			expected: stateCommitted,
		},
		{
			name:     "txID is 0: state is aborted",
			txID:     0,
			data:     byte(0b10000000),
			expected: stateAborted,
		},
		// byte offset is 1
		{
			name:     "txID is 1: state is in progress",
			txID:     1,
			data:     byte(0b01001000),
			expected: stateInProgress,
		},
		{
			name:     "txID is 1: state is committed",
			txID:     1,
			data:     byte(0b01011000),
			expected: stateCommitted,
		},
		{
			name:     "txID is 1: state is aborted",
			txID:     1,
			data:     byte(0b01101000),
			expected: stateAborted,
		},
		// byte offset is 2
		{
			name:     "txID is 2: state is in progress",
			txID:     2,
			data:     byte(0b01010000),
			expected: stateInProgress,
		},
		{
			name:     "txID is 2: state is committed",
			txID:     2,
			data:     byte(0b01000110),
			expected: stateCommitted,
		},
		{
			name:     "txID is 2: state is aborted",
			txID:     2,
			data:     byte(0b01101010),
			expected: stateAborted,
		},
		// byte offset is 3
		{
			name:     "txID is 3: state is in progress",
			txID:     3,
			data:     byte(0b01010000),
			expected: stateInProgress,
		},
		{
			name:     "txID is 3: state is committed",
			txID:     3,
			data:     byte(0b01000101),
			expected: stateCommitted,
		},
		{
			name:     "txID is 3: state is aborted",
			txID:     3,
			data:     byte(0b01101010),
			expected: stateAborted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getState(tt.data, tt.txID)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestGetUpdatedState(t *testing.T) {
	tests := []struct {
		name     string
		txID     txid.TxID
		data     byte
		state    state
		expected byte
	}{
		// byte offset is 0
		{
			name:     "txID is 0: state is in progress",
			txID:     0,
			data:     byte(0b01000000),
			state:    stateInProgress,
			expected: byte(0b00000000),
		},
		{
			name:     "txID is 0: state is committed",
			txID:     0,
			data:     byte(0b00000000),
			state:    stateCommitted,
			expected: byte(0b01000000),
		},
		{
			name:     "txID is 0: state is committed",
			txID:     0,
			data:     byte(0b00000000),
			state:    stateAborted,
			expected: byte(0b10000000),
		},
		// byte offset is 1
		{
			name:     "txID is 1: state is in progress",
			txID:     1,
			data:     byte(0b01011000),
			state:    stateInProgress,
			expected: byte(0b01001000),
		},
		{
			name:     "txID is 1: state is committed",
			txID:     1,
			data:     byte(0b01001000),
			state:    stateCommitted,
			expected: byte(0b01011000),
		},
		{
			name:     "txID is 1: state is aborted",
			txID:     1,
			data:     byte(0b01011000),
			state:    stateAborted,
			expected: byte(0b01101000),
		},
		// byte offset is 2
		{
			name:     "txID is 2: state is in progress",
			txID:     2,
			data:     byte(0b01011000),
			state:    stateInProgress,
			expected: byte(0b01010000),
		},
		{
			name:     "txID is 2: state is committed",
			txID:     2,
			data:     byte(0b01011000),
			state:    stateCommitted,
			expected: byte(0b01010100),
		},
		{
			name:     "txID is 2: state is aborted",
			txID:     2,
			data:     byte(0b01010000),
			state:    stateAborted,
			expected: byte(0b01011000),
		},
		// byte offset is 3
		{
			name:     "txID is 3: state is in progress",
			txID:     3,
			data:     byte(0b01010001),
			state:    stateInProgress,
			expected: byte(0b01010000),
		},
		{
			name:     "txID is 3: state is committed",
			txID:     3,
			data:     byte(0b01010000),
			state:    stateCommitted,
			expected: byte(0b01010001),
		},
		{
			name:     "txID is 3: state is aborted",
			txID:     3,
			data:     byte(0b01010001),
			state:    stateAborted,
			expected: byte(0b01010010),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getUpdatedState(tt.data, tt.txID, tt.state)
			assert.Equal(t, tt.expected, got)
		})
	}
}
