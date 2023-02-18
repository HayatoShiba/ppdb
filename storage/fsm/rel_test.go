package fsm

import (
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestGetAddressFromPageID(t *testing.T) {
	tests := []struct {
		name   string
		pageID relationPageID
		addr   address
		slot   fsmSlot
	}{
		{
			name:   "relation page id is 0",
			pageID: 0,
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			slot: 0,
		},
		{
			name:   "relation page id is 1",
			pageID: 1,
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			slot: 1,
		},
		{
			name:   "relation page id is leafNodeNum - 1",
			pageID: relationPageID(leafNodeNum - 1),
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			slot: fsmSlot(leafNodeNum - 1),
		},
		{
			name:   "relation page id is leafNodeNum",
			pageID: relationPageID(leafNodeNum),
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			slot: 0,
		},
		{
			name:   "relation page id is leafNodeNum + 1",
			pageID: relationPageID(leafNodeNum + 1),
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			slot: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAddr, gotSlot := getAddressFromRelationPageID(tt.pageID)
			assert.Equal(t, tt.addr, gotAddr)
			assert.Equal(t, tt.slot, gotSlot)
		})
	}
}

func TestGetRelationPageIDFromAddress(t *testing.T) {
	tests := []struct {
		name   string
		addr   address
		slot   fsmSlot
		pageID relationPageID
		ok     bool
	}{
		{
			name: "relation page id is 0",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			slot:   0,
			pageID: 0,
			ok:     true,
		},
		{
			name: "relation page id is 1,",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			slot:   1,
			pageID: 1,
			ok:     true,
		},
		{
			name: "relation page id is leafNodeNum - 1",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			slot:   fsmSlot(leafNodeNum - 1),
			pageID: relationPageID(leafNodeNum - 1),
			ok:     true,
		},
		{
			name: "relation page id is leafNodeNum",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			slot:   0,
			pageID: relationPageID(leafNodeNum),
			ok:     true,
		},
		{
			name: "relation page id is leafNodeNum + 1",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			slot:   1,
			pageID: relationPageID(leafNodeNum + 1),
			ok:     true,
		},
		{
			name: "when tree level is not bottom",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 1,
			},
			slot:   1,
			pageID: relationPageID(page.InvalidPageID),
			ok:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pageID, ok := getRelationPageIDFromAddress(tt.addr, tt.slot)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.pageID, pageID)
		})
	}
}
