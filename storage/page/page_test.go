package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPageInitialized(t *testing.T) {
	tests := []struct {
		name        string
		initialized bool
		expected    bool
	}{
		{
			name:        "page has not been initialized",
			initialized: false,
			expected:    false,
		},
		{
			name:        "page has been initialized",
			initialized: true,
			expected:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := NewPagePtr()
			if tt.initialized {
				InitializePage(page, 10)
			}
			got := IsInitialized(page)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestCalculateFreeSpace(t *testing.T) {
	page := NewPagePtr()
	InitializePage(page, 10)

	got := CalculateFreeSpace(page)
	expected := PageSize - slotsOffset - 10
	assert.Equal(t, int(expected), got)

	var item []byte = []byte{1, 2}
	err := AddItem(page, ItemPtr(item), InvalidSlotIndex)
	assert.Nil(t, err)

	got = CalculateFreeSpace(page)
	size := int(expected) - slotSize - len(item)
	assert.Equal(t, size, got)
}
