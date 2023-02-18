package fsm

import (
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestConvertToFreeSpaceSize(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		expected freeSpaceSize
		ok       bool
	}{
		{
			name:     "size is 0",
			size:     0,
			expected: 0,
			ok:       true,
		},
		{
			name:     "size is 31",
			size:     31,
			expected: 0,
			ok:       true,
		},
		{
			name:     "size is 32",
			size:     32,
			expected: 1,
			ok:       true,
		},
		{
			name:     "size is 33",
			size:     33,
			expected: 1,
			ok:       true,
		},
		{
			name:     "size is 8192",
			size:     8192,
			expected: 255,
			ok:       true,
		},
		{
			name:     "size is 8193",
			size:     8193,
			expected: 0,
			ok:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fss, ok := convertToFreeSpaceSize(tt.size)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.expected, fss)
		})
	}
}

func TestGetFreeSpaceSizeFromNodeIndex(t *testing.T) {
	p := page.NewPagePtr()

	var nidx nodeIndex = 2
	var expected freeSpaceSize = 10
	updateFreeSpaceSizeFromNodeIndex(p, nidx, expected)
	got := getFreeSpaceSizeFromNodeIndex(p, nidx)
	assert.Equal(t, expected, got)
}
