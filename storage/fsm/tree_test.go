package fsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLeftChildNode(t *testing.T) {
	tests := []struct {
		name        string
		parentIndex nodeIndex
		expected    nodeIndex
	}{
		{
			name:        "root node's child",
			parentIndex: 0,
			expected:    1,
		},
		{
			name:        "child node's child",
			parentIndex: 3,
			expected:    7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getLeftChildNode(tt.parentIndex)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestGetRightChildNode(t *testing.T) {
	tests := []struct {
		name        string
		parentIndex nodeIndex
		expected    nodeIndex
	}{
		{
			name:        "root node's child",
			parentIndex: 0,
			expected:    2,
		},
		{
			name:        "child node's child",
			parentIndex: 3,
			expected:    8,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getRightChildNode(tt.parentIndex)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestGetParentNode(t *testing.T) {
	tests := []struct {
		name       string
		childIndex nodeIndex
		expected   nodeIndex
	}{
		{
			name:       "right child node",
			childIndex: 3,
			expected:   1,
		},
		{
			name:       "left child node",
			childIndex: 4,
			expected:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getParentNode(tt.childIndex)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestGetSlotFromNodeIndex(t *testing.T) {
	tests := []struct {
		name  string
		index nodeIndex
		slot  fsmSlot
		ok    bool
	}{
		{
			name:  "non-leaf node: 4090",
			index: 4090,
			slot:  0,
			ok:    false,
		},
		{
			name:  "first leaf node: 4091",
			index: 4091,
			slot:  0,
			ok:    true,
		},
		{
			name:  "second leaf node: 4092",
			index: 4092,
			slot:  1,
			ok:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, ok := getSlotFromNodeIndex(tt.index)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.slot, slot)
		})
	}
}

func TestIsLeaf(t *testing.T) {
	tests := []struct {
		name     string
		index    nodeIndex
		expected bool
	}{
		{
			name:     "non-leaf node: 4090",
			index:    4090,
			expected: false,
		},
		{
			name:     "first leaf node: 4091",
			index:    4091,
			expected: true,
		},
		{
			name:     "second leaf node: 4092",
			index:    4092,
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isLeaf(tt.index))
		})
	}
}

func TestIsRoot(t *testing.T) {
	tests := []struct {
		name     string
		index    nodeIndex
		expected bool
	}{
		{
			name:     "root node: 0",
			index:    rootNodeIndex,
			expected: true,
		},
		{
			name:     "non-root node: 1",
			index:    1,
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isRoot(tt.index))
		})
	}
}
