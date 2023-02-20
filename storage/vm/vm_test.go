package vm

import (
	"testing"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestGetVMPageIDFromPageID(t *testing.T) {
	tests := []struct {
		name     string
		pageID   page.PageID
		vmPageID page.PageID
	}{
		{
			name:     "page id is 0",
			pageID:   0,
			vmPageID: 0,
		},
		{
			name:     "page id is nodeNumPerPage-1",
			pageID:   page.PageID(nodeNumPerPage - 1),
			vmPageID: 0,
		},
		{
			name:     "page id is nodeNumPerPage",
			pageID:   page.PageID(nodeNumPerPage),
			vmPageID: 1,
		},
		{
			name:     "page id is nodeNumPerPage+1",
			pageID:   page.PageID(nodeNumPerPage + 1),
			vmPageID: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmPageID := GetVMPageIDFromPageID(tt.pageID)
			assert.Equal(t, tt.vmPageID, vmPageID)
		})
	}
}

func TestGetStatusUpdateStatus(t *testing.T) {
	p := page.NewPagePtr()
	tests := []struct {
		name   string
		pageID page.PageID
		flags  uint8
	}{
		// byte offset is 0
		{
			name:   "page id is 0, flag is all visible",
			pageID: 0,
			flags:  StatusAllVisible,
		},
		{
			name:   "page id is 0, flag is normal",
			pageID: 0,
			flags:  StatusInitialized,
		},
		{
			name:   "page id is 0, flag is all frozen",
			pageID: 0,
			flags:  StatusAllFrozen,
		},
		// byte offset is 1
		{
			name:   "page id is 1, flag is all visible",
			pageID: 1,
			flags:  StatusAllVisible,
		},
		{
			name:   "page id is 1, flag is normal",
			pageID: 1,
			flags:  StatusInitialized,
		},
		{
			name:   "page id is 1, flag is all frozen",
			pageID: 1,
			flags:  StatusAllFrozen,
		},
		// byte offset is 2
		{
			name:   "page id is 2, flag is all visible",
			pageID: 2,
			flags:  StatusAllVisible,
		},
		{
			name:   "page id is 2, flag is normal",
			pageID: 2,
			flags:  StatusInitialized,
		},
		{
			name:   "page id is 2, flag is all frozen",
			pageID: 2,
			flags:  StatusAllFrozen,
		},
		// byte offset is 3
		{
			name:   "page id is 3, flag is all visible",
			pageID: 3,
			flags:  StatusAllVisible,
		},
		{
			name:   "page id is 3, flag is normal",
			pageID: 3,
			flags:  StatusInitialized,
		},
		{
			name:   "page id is 3, flag is all frozen",
			pageID: 3,
			flags:  StatusAllFrozen,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			UpdateStatus(p, tt.pageID, tt.flags)
			got := GetStatus(p, tt.pageID)
			assert.Equal(t, tt.flags, got)
		})
	}
}
