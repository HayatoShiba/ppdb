package clog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateLRUcount(t *testing.T) {
	bm, err := TestingNewBufferManager(t)
	assert.Nil(t, err)

	tests := []struct {
		name     string
		bufID    bufferID
		lruCount uint64
	}{
		{
			name:     "access once",
			bufID:    0,
			lruCount: firstLRUCount + 1,
		},
		{
			name:     "access twice",
			bufID:    1,
			lruCount: firstLRUCount + 2,
		},
		{
			name:     "access third",
			bufID:    2,
			lruCount: firstLRUCount + 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bm.updateLRUcount(tt.bufID)
			assert.Equal(t, tt.lruCount, bm.descriptors[tt.bufID].lruCount)
		})
	}
}
