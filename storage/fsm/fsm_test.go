package fsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPageIDFromAddr(t *testing.T) {
	tests := []struct {
		name      string
		addr      address
		fsmPageID fsmPageID
	}{
		{
			name: "tree level is root, logical page id is 0",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 0,
			},
			fsmPageID: 0,
		},
		{
			name: "tree level is root, logical page id is 1",
			addr: address{
				treeLevel:     treeLevelRoot,
				logicalPageID: 1,
			},
			fsmPageID: fsmPageID(leafNodeNum*leafNodeNum + leafNodeNum + 1),
		},
		{
			name: "tree level is bottom, logical page id is 0",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 0,
			},
			fsmPageID: 2,
		},
		{
			name: "tree level is bottom, logical page id is 1",
			addr: address{
				treeLevel:     treeLevelBottom,
				logicalPageID: 1,
			},
			fsmPageID: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsmPageID := getFSMPageIDFromAddress(tt.addr)
			assert.Equal(t, tt.fsmPageID, fsmPageID)
		})
	}
}
