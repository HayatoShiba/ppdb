package buffer

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/pkg/errors"
)

// ReadBufferFSM reads fsm page into buffer
// this returns buffer after acquiring pin and content lock
// if exclusive is true, acquire exclusive content lock
func (m *Manager) ReadBufferFSM(rel common.Relation, pageID page.PageID, exclusive bool) (BufferID, error) {
	// if fsm page does not exist yet, extend the page
	// for the nature of page tree structure of free space map, it may have to extend multiple pages
	npid, err := m.dm.GetNPageID(rel, disk.ForkNumberFSM)
	if err != nil {
		return InvalidBufferID, errors.Wrap(err, "GetNPageID failed")
	}

	if pageID > npid {
		// has to extend the fsm page until pageID
		// maybe this logic can be optimized
		for i := npid; i < pageID; i++ {
			if _, err = m.dm.ExtendPage(rel, disk.ForkNumberFSM, false); err != nil {
				return InvalidBufferID, errors.Wrap(err, "dm.ExtendPage failed")
			}
		}
	}

	bufID, err := m.ReadBuffer(rel, disk.ForkNumberFSM, pageID)
	if err != nil {
		return InvalidBufferID, errors.Wrap(err, "ReadBuffer failed")
	}
	if exclusive {
		m.descriptors[bufID].contentLock.Lock()
	} else {
		m.descriptors[bufID].contentLock.RLock()
	}
	return bufID, nil
}

// ReleaseBufferFSM releases fsm page
// if exclusive is true, release content exclusive lock
func (m *Manager) ReleaseBufferFSM(bufID BufferID, exclusive bool) {
	if exclusive {
		m.descriptors[bufID].contentLock.Unlock()
	} else {
		m.descriptors[bufID].contentLock.RUnlock()
	}
	m.ReleaseBuffer(bufID)
}
