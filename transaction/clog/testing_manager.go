package clog

import (
	"testing"

	"github.com/pkg/errors"
)

func TestingNewDiskManager(t *testing.T) (*diskManager, error) {
	dir = t.TempDir()
	return newDiskManager()
}

func TestingNewBufferManager(t *testing.T) (*bufferManager, error) {
	dm, err := TestingNewDiskManager(t)
	if err != nil {
		return nil, errors.Wrap(err, "TestingNewDiskManager failed")
	}
	return newBufferManager(dm), nil
}

// TestingNewManager initializes manager
func TestingNewManager(t *testing.T) (Manager, error) {
	bm, err := TestingNewBufferManager(t)
	if err != nil {
		return nil, errors.Wrap(err, "TestingNewBufferManager failed")
	}
	return &ManagerImpl{bm}, nil
}
