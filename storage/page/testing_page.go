package page

import (
	"github.com/pkg/errors"
)

func TestingNewRandomPage() (PagePtr, error) {
	p := NewPagePtr()
	InitializePage(p, 10)

	// insert
	item := []byte{1, 2, 3, 4, 5, 6}
	if err := AddItem(p, item, InvalidSlotIndex); err != nil {
		return nil, errors.Wrap(err, "AddItem failed")
	}

	// insert
	item = []byte{8, 9}
	if err := AddItem(p, item, InvalidSlotIndex); err != nil {
		return nil, errors.Wrap(err, "AddItem failed")
	}

	return p, nil
}
