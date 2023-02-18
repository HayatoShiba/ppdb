package fsm

import (
	"github.com/HayatoShiba/ppdb/storage/buffer"
	"github.com/pkg/errors"
)

// TestingNewManager initializes the shared buffer manager
func TestingNewManager() (*Manager, error) {
	bm, err := buffer.TestingNewManager()
	if err != nil {
		return nil, errors.Wrap(err, "buffer.TestingNewManager failed")
	}
	return NewManager(bm), nil
}
