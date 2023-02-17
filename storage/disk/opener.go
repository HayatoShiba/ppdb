/*
This file defines opener interface and its implementations.
We don't want to execute disk I/O in test, so it's better to use byte slice instead of actual file in test.
For this reason, opener interface is defined. Opener opens its storage. The implementations are:
- fileOpener: open and return file.
- bufferOpener: open and return byte slice. this is intended to be used in test.
*/
package disk

import (
	"os"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/pkg/errors"
)

// opener opens storage
type opener interface {
	open(common.Relation, ForkNumber) (storage, error)
}

// fileOpener opens file
// maybe should be better name
type fileOpener struct {
	// cache file descriptors after open the files
	st map[string]storage
}

// newFileOpener initializes fileOpener
func newFileOpener() *fileOpener {
	return &fileOpener{
		st: make(map[string]storage),
	}
}

// open opens and returns specified database file under base directory
func (fo *fileOpener) open(rel common.Relation, forkNum ForkNumber) (storage, error) {
	filePath := getRelationForkFilePath(rel, forkNum)
	// when file descriptor is cached, just return it
	st, ok := fo.st[filePath]
	if ok {
		return st, nil
	}
	fd, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "os.OpenFile failed")
	}
	// cache file descriptor when open the file
	fo.st[filePath] = fileStorage{fd}
	return fileStorage{fd}, nil
}

// bufferOpener opens buffer
type bufferOpener struct {
	st map[string]storage
}

// newBufferOpener initializes bufferOpener
func newBufferOpener() *bufferOpener {
	return &bufferOpener{
		st: make(map[string]storage),
	}
}

// open returns specified buffer
func (bo *bufferOpener) open(rel common.Relation, forkNum ForkNumber) (storage, error) {
	path := getRelationForkFilePath(rel, forkNum)
	buf, ok := bo.st[path]
	if ok {
		return buf, nil
	}
	buf = newBufferStorage()
	bo.st[path] = buf
	return buf, nil
}
