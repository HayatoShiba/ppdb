package buffer

import "github.com/HayatoShiba/ppdb/storage/page"

// buffer is byte array
// page is fetched from disk into this
type buffer *[bufferSize]byte

// newBuffers initializes buffer pool
func newBuffers() [bufferNum]buffer {
	var buffers [bufferNum]buffer
	for i := 0; i < bufferNum; i++ {
		buffers[i] = &[bufferSize]byte{}
	}
	return buffers
}

const (
	// the size of one buffer.
	// this must be equal to page size because page is fetched into buffer
	// buffer-related metadata is managed in different structure called `buffer descriptor`
	bufferSize = page.PageSize

	// bufferPoolSize is the size of shared buffer pool
	// default in postgres is 32MB
	// see shared_buffers parameter in the link below
	// https://www.postgresql.org/docs/9.1/runtime-config-resource.html
	// in ppdb, 1MB is enough probably
	bufferPoolSize = 1000000

	// the number of buffers which is managed by shared buffer pool manager
	bufferNum = bufferPoolSize / bufferSize
)
