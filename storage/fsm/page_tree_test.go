package fsm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetChildAddress(t *testing.T) {
	tests := []struct {
		name         string
		addr         address
		slot         fsmSlot
		expectedAddr address
		ok           bool
	}{
		{
			name: "parent's logical page id is 0, slot is 0, tree level is root",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 0,
			},
			slot: 0,
			expectedAddr: address{
				treeLevel:     treeLevelRoot - 1,
				logicalPageID: 0,
			},
			ok: true,
		},
		{
			name: "parent's logical page id is 0, slot is 1, tree level is root",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 0,
			},
			slot: 1,
			expectedAddr: address{
				treeLevel:     treeLevelRoot - 1,
				logicalPageID: 1,
			},
			ok: true,
		},
		{
			name: "parent's logical page id is 1, slot is 0, tree level is root",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 1,
			},
			slot: 0,
			expectedAddr: address{
				treeLevel:     treeLevelRoot - 1,
				logicalPageID: logicalPageID(leafNodeNum),
			},
			ok: true,
		},
		{
			name: "parent's logical page id is 1, slot is 1, tree level is root",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 1,
			},
			slot: 1,
			expectedAddr: address{
				treeLevel:     treeLevelRoot - 1,
				logicalPageID: logicalPageID(leafNodeNum) + 1,
			},
			ok: true,
		},
		{
			name: "parent's logical page id is 1, slot is 1, tree level is middle",
			addr: address{
				treeLevel:     treeLevelRoot - 1,
				logicalPageID: 1,
			},
			slot: 1,
			expectedAddr: address{
				treeLevel:     treeLevelRoot - 2,
				logicalPageID: logicalPageID(leafNodeNum) + 1,
			},
			ok: true,
		},
		{
			name: "parent is bottom level",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			slot:         1,
			expectedAddr: address{},
			ok:           false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, ok := getChildAddress(tt.addr, tt.slot)
			assert.Equal(t, tt.ok, ok)
			assert.True(t, reflect.DeepEqual(tt.expectedAddr, addr))
		})
	}
}

func TestGetParentAddress(t *testing.T) {
	tests := []struct {
		name         string
		addr         address
		expectedAddr address
		expectedSlot fsmSlot
		ok           bool
	}{
		{
			name: "child's logical page id is 0, tree level is root",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			expectedAddr: address{
				treeLevel:     treeLevelBottom + 1,
				logicalPageID: 0,
			},
			expectedSlot: 0,
			ok:           true,
		},
		{
			name: "child's logical page id is 1, tree level is root",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			expectedAddr: address{
				treeLevel:     treeLevelBottom + 1,
				logicalPageID: 0,
			},
			expectedSlot: 1,
			ok:           true,
		},
		{
			name: "child's logical page id is leafNodeNum, slot is 1, tree level is root",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: logicalPageID(leafNodeNum),
			},
			expectedAddr: address{
				treeLevel:     treeLevelBottom + 1,
				logicalPageID: 0,
			},
			ok:           true,
			expectedSlot: fsmSlot(leafNodeNum),
		},
		{
			name: "child's logical page id is leafNodeNum+1, slot is 1, tree level is middle",
			addr: address{
				treeLevel:     treeLevelRoot - 1,
				logicalPageID: logicalPageID(leafNodeNum) + 1,
			},
			expectedAddr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 1,
			},
			ok:           true,
			expectedSlot: 0,
		},
		{
			name: "child is root level",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 1,
			},
			expectedAddr: address{},
			ok:           false,
			expectedSlot: invalidSlot,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, slot, ok := getParentAddress(tt.addr)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.expectedSlot, slot)
			assert.True(t, reflect.DeepEqual(tt.expectedAddr, addr))
		})
	}
}
