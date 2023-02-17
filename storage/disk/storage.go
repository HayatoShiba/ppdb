/*
This file defines storage interface and its implementations.
We don't want to execute disk I/O in test, so it's better to use byte slice instead of actual file in test.
For this reason, storage interface is defined. Possible operation with storage is read/write/seek/sync/get size.
(TODO: implement close method)
The implementations are:
- fileStorage: wrapper of os.File
- bufferStorage: this consists of byte slice and the current position of the byte slice.

note:
- bytes.Buffer doesn't implement io.Seeker because it is designed to read data in buffer once.
- bytes.Reader doesn't implement io.Writer
- so it may be better to define bufferStorage by myself.
*/
package disk

import (
	"io"
	"os"

	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/pkg/errors"
)

// storage is storage which implements multiple operations necessary for ppdb database file.
type storage interface {
	io.ReadWriteSeeker
	Size() (int64, error)
	Sync() error
}

// fileStorage is file storage
type fileStorage struct {
	*os.File
}

// Size returns the storage's size
func (fs fileStorage) Size() (int64, error) {
	stat, err := fs.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "Stat failed")
	}
	return stat.Size(), nil
}

// bufferStorage is buffer storage
type bufferStorage struct {
	// buf is actual contents
	buf []byte
	// off is current position
	// (is int enough to store?)
	off int
}

// newBufferStorage initializes bufferStorage
func newBufferStorage() *bufferStorage {
	// initialize with one page size
	buf := make([]byte, page.PageSize)
	return &bufferStorage{
		buf: buf,
		off: 0,
	}
}

// Size returns the buffer size
func (bs *bufferStorage) Size() (int64, error) {
	size := len(bs.buf)
	return int64(size), nil
}

// Sync doesn't do anything
func (bs *bufferStorage) Sync() error {
	// on-memory byte slice doesn't need sync
	return nil
}

// Read reads buffer at current position into p
func (bs *bufferStorage) Read(p []byte) (n int, err error) {
	nread := copy(p, bs.buf[bs.off:])
	if nread != len(p) {
		return nread, errors.Errorf("cannot fully read: nread %d, len %d", nread, len(p))
	}
	bs.off = bs.off + nread
	return nread, nil
}

// Write writes p into buffer at current position
func (bs *bufferStorage) Write(p []byte) (n int, err error) {
	// if the buffer is EOF, then extend the byte slice with page size
	if len(bs.buf) == bs.off {
		pg := page.NewPagePtr()
		bs.buf = append(bs.buf, pg[:]...)
	}
	nwritten := copy(bs.buf[bs.off:], p)
	if nwritten != len(p) {
		return nwritten, errors.Errorf("cannot fully written: nread %d, len %d", nwritten, len(p))
	}
	bs.off = bs.off + nwritten
	return nwritten, nil
}

// Seek seeks and moves buffer off
func (bs *bufferStorage) Seek(offset int64, whence int) (int64, error) {
	if whence != os.SEEK_SET {
		return 0, errors.Errorf("whence is unexpected: %d", whence)
	}
	bs.off = int(offset)
	return offset, nil
}
