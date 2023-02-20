package clog

import (
	"testing"

	"github.com/HayatoShiba/ppdb/transaction/txid"
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

// TestingNewManager initializes manager
func TestingNewMockManager() (Manager, error) {
	return NewMockManagerImpl(), nil
}

type MockManagerImpl struct {
	states map[txid.TxID]state
}

func NewMockManagerImpl() Manager {
	return &MockManagerImpl{
		states: make(map[txid.TxID]state),
	}
}

func (mmi *MockManagerImpl) IsTxCommitted(txID txid.TxID) (bool, error) {
	st, ok := mmi.states[txID]
	if !ok {
		return false, nil
	}
	if st == stateCommitted {
		return true, nil
	}
	return false, nil
}

func (mmi *MockManagerImpl) IsTxAborted(txID txid.TxID) (bool, error) {
	st, ok := mmi.states[txID]
	if !ok {
		return false, nil
	}
	if st == stateAborted {
		return true, nil
	}
	return false, nil
}

func (mmi *MockManagerImpl) SetStateCommitted(txID txid.TxID) error {
	mmi.states[txID] = stateCommitted
	return nil
}

func (mmi *MockManagerImpl) SetStateAborted(txID txid.TxID) error {
	mmi.states[txID] = stateAborted
	return nil
}
