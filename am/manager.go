/*
Access method
Currently in ppdb, only heap access method is supported. (index is not supported)

heap access methods are defined below
https://github.com/postgres/postgres/blob/8e1db29cdbbd218ab6ba53eea56624553c3bef8c/src/backend/access/heap/heapam_handler.c#L2532-L2589
*/
package am

import (
	"github.com/HayatoShiba/ppdb/storage/buffer"
	"github.com/HayatoShiba/ppdb/storage/fsm"
	"github.com/HayatoShiba/ppdb/transaction/snapshot"
)

type Manager struct {
	bm  *buffer.Manager
	sm  *snapshot.Manager
	fsm fsm.Manager
}

// NewManager initializes access manager
func NewManager(bm *buffer.Manager, sm *snapshot.Manager, fsm fsm.Manager) *Manager {
	return &Manager{
		bm:  bm,
		sm:  sm,
		fsm: fsm,
	}
}
