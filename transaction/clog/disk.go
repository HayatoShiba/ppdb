// this disk manager is very simple and fix later like main disk manager.
package clog

import (
	"io"
	"os"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/pkg/errors"
)

var (
	// the file path of clog
	dir  = "pg_xact"
	path = "/clog"
)

// diskManager manages clog file
type diskManager struct {
	fd *os.File
}

// newDiskManager initializes the clog disk manager
func newDiskManager() (*diskManager, error) {
	// check whether the directory already exists
	if _, err := os.Stat(dir); !os.IsExist(err) {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, errors.Wrap(err, "os.MkdirAll failed")
		}
	}

	// TODO: refactor file path
	fd, err := os.OpenFile(dir+path, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "os.OpenFile failed")
	}
	return &diskManager{
		fd: fd,
	}, nil
}

// writePage writes page out to disk
// see https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L757
func (dm *diskManager) writePage(pageID page.PageID, p page.PagePtr) error {
	n, err := dm.fd.WriteAt(p[:], page.CalculateFileOffset(pageID))
	if err != nil {
		return errors.Wrap(err, "WriteAt failed")
	}
	if n != page.PageSize {
		return errors.Errorf("WriteAt failed to write the whole page: %d", n)
	}
	// note: send sync request for checkpointer?
	// https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L891
	return nil
}

// readPage reads page from disk
func (dm *diskManager) readPage(pageID page.PageID, p page.PagePtr) error {
	n, err := dm.fd.ReadAt(p[:], page.CalculateFileOffset(pageID))
	if err != nil {
		// if file is EOF, extend it
		if err == io.EOF {
			// when the file has already been extend to the max page id, it cannot be extended anymore
			for {
				pid, err := dm.extendPage()
				if err != nil {
					return errors.Wrap(err, "extendPage failed")
				}
				if pid == pageID {
					// return zero-filled page
					p = page.NewPagePtr()
					return nil
				}
			}
		}
		return errors.Wrap(err, "ReadAt failed")
	}
	if n != page.PageSize {
		return errors.Errorf("ReadAt failed to read the whole page: %d", n)
	}
	return nil
}

// extendPage extends page and returns the new pageID
func (dm *diskManager) extendPage() (page.PageID, error) {
	pageID, err := dm.getNPageID()
	if err != nil {
		return page.InvalidPageID, errors.Wrap(err, "getNPageID failed")
	}

	// when the file has already been extend to the max page id, it cannot be extended anymore
	if pageID == page.MaxPageID {
		return pageID, errors.New("the page is MaxPageID and cannot be extended anymore")
	}

	pid := pageID + 1
	if err := dm.writePage(pid, page.NewPagePtr()); err != nil {
		return page.InvalidPageID, errors.Wrap(err, "WritePage failed")
	}
	return pid, nil
}

// GetNPageID returns the last PageID of the file
func (dm *diskManager) getNPageID() (page.PageID, error) {
	fi, err := dm.fd.Stat()
	if err != nil {
		return page.InvalidPageID, errors.Wrap(err, "f.Stat failed")
	}
	size := fi.Size()
	if size == 0 {
		return page.InvalidPageID, nil
	}
	lastPageID := (size / page.PageSize) - 1
	return page.PageID(lastPageID), nil
}
