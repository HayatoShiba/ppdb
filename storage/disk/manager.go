/*
Disk manager deals with the files under base directory.
This mainly manages table files/fsm files/vm files/(index files if index is implemented).

note: pg_xact directory (clog) and pg_wal directory (wal) are not managed by this manager.
To differentiate from clog/wal disk manager, this disk manager may be called main disk manager in ppdb.

The implementation of disk manager is based on src/backend/storage/smgr directory in postgres.
See smgr README https://github.com/postgres/postgres/blob/b0a55e43299c4ea2a9a8c757f9c26352407d0ccc/src/backend/storage/smgr/README#L1

Postgres seems to manage file descriptors by itself not to exceed system limits on the number of open files a single process can have.
This may be called `virtual file descriptor` see https://github.com/postgres/postgres/blob/2d4f1ba6cfc2f0a977f1c30bda9848041343e248/src/backend/storage/file/fd.c#L1-L71

ppdb does not support
- database and schema (so CREATE DATABASE and CREATE SCHEMA is not supported)
- the division of files into segments (see https://github.com/postgres/postgres/blob/85d8b30724c0fd117a683cc72706f71b28463a05/src/backend/storage/smgr/md.c#L44-L80
- ...
*/
package disk

import (
	"os"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/pkg/errors"
)

// the directory path of database files
// table files, fsm files, vm files, (index files) are located under this directory
// the path of the table file in postgres is
// - base/database oid/table oid/files
// ppdb supports only table, so the path of the table file in ppdb is
// - base/database/table oid/files
var baseDir = "base/database"

// Manager manages disk
type Manager struct {
	// cache file descriptors when open the files
	fds map[string]*os.File
}

// NewManager initializes disk manager
func NewManager() (*Manager, error) {
	// check whether the directory already exists
	if _, err := os.Stat(baseDir); !os.IsExist(err) {
		if err := os.MkdirAll(baseDir, 0700); err != nil {
			return nil, errors.Wrap(err, "os.MkdirAll failed")
		}
	}

	return &Manager{
		fds: make(map[string]*os.File),
	}, nil
}

// ReadPage reads page from disk into page.PagePtr
func (m *Manager) ReadPage(pageID page.PageID, p page.PagePtr) error {
	offset := page.CalculateFileOffset(pageID)
	// TODO: fix getDatabaseFile() later. this is temporary-defined function.
	f := getDatabaseFile()

	n, err := f.ReadAt(p[:], offset)
	if err != nil {
		return errors.Wrap(err, "ReadAt failed")
	}
	if n != len(p) {
		return errors.Errorf("ReadAt failed to read the whole page: %d, page length is %d", n, len(p))
	}
	return nil
}

// WritePage writes page out to disk
// see https://github.com/postgres/postgres/blob/85d8b30724c0fd117a683cc72706f71b28463a05/src/backend/storage/smgr/md.c#L738
func (m *Manager) WritePage(pageID page.PageID, p page.PagePtr, skipFsync bool) error {
	offset := page.CalculateFileOffset(pageID)
	// TODO: fix getDatabaseFile() later. this is temporary-defined function.
	f := getDatabaseFile()

	n, err := f.WriteAt(p[:], offset)
	if err != nil {
		return errors.Wrap(err, "WriteAt failed")
	}
	if n != len(p) {
		return errors.Errorf("WriteAt failed to write the whole page: %d, the page length is %d", n, len(p))
	}

	if !skipFsync {
		// postgres seems to send request to checkpointer at first?
		// see https://github.com/postgres/postgres/blob/85d8b30724c0fd117a683cc72706f71b28463a05/src/backend/storage/smgr/md.c#L789
		if err := f.Sync(); err != nil {
			return errors.Wrap(err, "Sync failed")
		}
	}
	return nil
}

// temporary defined function. this returns the file
var getDatabaseFile = func() *os.File {
	return nil
}
