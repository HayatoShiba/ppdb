package fsm

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/buffer"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/pkg/errors"
)

// TestingNewManager initializes the shared buffer manager
func TestingNewManager() (Manager, error) {
	bm, err := buffer.TestingNewManager()
	if err != nil {
		return nil, errors.Wrap(err, "buffer.TestingNewManager failed")
	}
	return NewManager(bm), nil
}

type MockManager struct {
	pageID page.PageID
	isErr  bool
}

func TestingNewMockManager(pageID page.PageID, isErr bool) Manager {
	return &MockManager{
		pageID: pageID,
		isErr:  isErr,
	}
}

func (mm *MockManager) SearchPageIDWithFreeSpaceSize(rel common.Relation, size int) (page.PageID, error) {
	if mm.isErr {
		return page.InvalidPageID, errors.New("mock errors")
	}
	return mm.pageID, nil
}

func (mm *MockManager) UpdateFSM(rel common.Relation, pageID page.PageID, size int) error {
	if mm.isErr {
		return errors.New("mock errors")
	}
	return nil
}
