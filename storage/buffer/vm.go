package buffer

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/vm"
	"github.com/pkg/errors"
)

// UpdateVMStatus updates the vm bits corresponding to the pageID
func (m *Manager) UpdateVMStatus(rel common.Relation, pageID page.PageID, flags uint8) error {
	// convert the page id to vm page id
	vmPageID := vm.GetVMPageIDFromPageID(pageID)

	// if vm page does not exist yet, extend the page
	// for the nature of visibility map, it may have to extend multiple pages
	npid, err := m.dm.GetNPageID(rel, disk.ForkNumberVM)
	if err != nil {
		return errors.Wrap(err, "GetNPageID failed")
	}
	// TODO: this is temporary defined. fix GetNPageID
	if npid == page.InvalidPageID {
		npid = 0
	}

	// TODO: should be >. not >=. fix GetNPageID.
	if vmPageID >= npid {
		// has to extend the vm page until the page id reaches the pageID
		// maybe this logic can be optimized
		// TODO: should be <. not <=. fix GetNPageID.
		for i := npid; i <= vmPageID; i++ {
			if _, err = m.dm.ExtendPage(rel, disk.ForkNumberVM, false); err != nil {
				return errors.Wrap(err, "dm.ExtendPage failed")
			}
			// have to initialize page
		}
	}

	// fetch the vm page into buffer
	bufferID, err := m.ReadBuffer(rel, disk.ForkNumberVM, vmPageID)
	if err != nil {
		return errors.Wrap(err, "ReadBuffer failed")
	}
	// acquire content lock
	m.descriptors[bufferID].contentLock.Lock()

	p := page.PagePtr(m.buffers[bufferID][:])
	if !page.IsInitialized(p) {
		page.InitializePage(p, 0)
	}

	// update bits of the pageID
	vm.UpdateStatus(p, pageID, flags)

	m.descriptors[bufferID].setDirty()
	m.descriptors[bufferID].contentLock.Unlock()
	m.ReleaseBuffer(bufferID)
	return nil
}

// GetVMStatus gets the status of VM bits
func (m *Manager) GetVMStatus(rel common.Relation, pageID page.PageID) (uint8, error) {
	// convert the page id to vm page id
	vmPageID := vm.GetVMPageIDFromPageID(pageID)
	// if vm page does not exist yet, just return initialized
	npid, err := m.dm.GetNPageID(rel, disk.ForkNumberVM)
	if err != nil {
		return vm.StatusInitialized, errors.Wrap(err, "GetNPageID failed")
	}
	if vmPageID > npid {
		return vm.StatusInitialized, nil
	}

	// fetch the vm page into buffer
	bufferID, err := m.ReadBuffer(rel, disk.ForkNumberVM, vmPageID)
	if err != nil {
		return vm.StatusInitialized, errors.Wrap(err, "ReadBuffer failed")
	}
	// acquire content lock
	m.descriptors[bufferID].contentLock.RLock()

	// get the status
	status := vm.GetStatus(page.PagePtr(m.buffers[bufferID][:]), pageID)

	m.descriptors[bufferID].contentLock.RUnlock()
	m.ReleaseBuffer(bufferID)

	return status, nil
}
