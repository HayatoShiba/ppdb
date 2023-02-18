/*
Clog manager manages clog.
Clog is stored under pg_xact directory. In ppdb, only one file exists for clog under pg_xact directory.

----
About clog

Clog stores all transaction status,
and the reason why transaction status is necessary is described at comment at /storage/buffer/manager.go.
Simply put, the visibility of tuples cannot be determined without clog.

----
About clog buffer manager

The cache eviction policy is LRU(least-recently-used) while shared buffer uses clock-sweep algorithm.
The access pattern of clog is predictable to some extent.

- write operation is mostly to the latest clog page
  - so the latest page should not be evicted.

- read operation is basically to the small number of pages

see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L3

----
About clog interface

- check the transaction status, whether the transaction has been committed or aborted
- write the transaction status to clog file when the transaction is committed/aborted

----
About Vacuum

TODO: when vacuum, clog segments are truncated.
https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L878

see https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L3
*/
package clog

// Manager is clog manager
type Manager struct {
}

// NewManager initializes clog manager
func NewManager() *Manager {
	return &Manager{}
}
