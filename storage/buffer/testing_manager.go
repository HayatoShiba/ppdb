package buffer

import (
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/pkg/errors"
)

// TestingNewManager initializes the shared buffer manager
func TestingNewManager() (*Manager, error) {
	dm, err := disk.TestingNewBufferManager()
	if err != nil {
		return nil, errors.Wrap(err, "disk.TestingNewBufferManager failed")
	}
	return NewManager(dm), nil
}

// TestingNewManagerWithNoFreeList initializes the shared buffer manager with no free list
func TestingNewManagerWithNoFreeList() (*Manager, error) {
	dm, err := disk.TestingNewBufferManager()
	if err != nil {
		return nil, errors.Wrap(err, "disk.TestingNewBufferManager failed")
	}
	m := NewManager(dm)
	m.freeList = freeListInvalidID
	return m, nil
}

// TestingNewManagerWithOneElementInFreeList initializes the shared buffer manager with one element in free list
func TestingNewManagerWithOneElementInFreeList() (*Manager, error) {
	dm, err := disk.TestingNewBufferManager()
	if err != nil {
		return nil, errors.Wrap(err, "disk.TestingNewBufferManager failed")
	}
	m := NewManager(dm)
	m.freeList = FirstBufferID
	m.descriptors[FirstBufferID].nextFreeID = freeListInvalidID
	return m, nil
}
