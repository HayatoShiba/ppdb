package clog

import (
	"sync"

	"github.com/HayatoShiba/ppdb/storage/page"
)

// bufferDescriptor is buffer descriptor
type bufferDescriptor struct {
	// pageID is what page the buffer stores
	pageID page.PageID
	// status is buffer status
	status bufferStatus
	// dirty indicates whether the buffer is written
	dirty bool

	// https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/access/slru.h#L65
	// statusのread in progressとか何？
	// per buffer lock for io
	// probably this lock is separated from the whole buffer lock because io is time-consuming
	sync.Mutex
	// 何してるんだこれ
	// なんかおかしくない？
	// https://github.com/postgres/postgres/blob/5ca3645cb3fb4b8b359ea560f6a1a230ea59c8bc/src/backend/access/transam/slru.c#L346-L350
}

// newBufferDescriptors initializes buffer descriptors
// this is expected to be called during initialization
func newBufferDescriptors() [bufferNum]*bufferDescriptor {
	descs := [bufferNum]*bufferDescriptor{}
	for i := 0; i < bufferNum; i++ {
		descs[i] = &bufferDescriptor{}
	}
	return descs
}

// bufferStatus is buffer status
type bufferStatus uint

const (
	// buffer has not been used
	bufferStatusEmpty bufferStatus = iota
	// buffer has been used
	bufferStatusUsed
	// read io in progress at the buffer
	// probably, this is for allowing other goroutines to know the io status without acquiring per-buffer lock
	bufferStatusReadIOInProgress
	bufferStatusWriteIOInProgress
)
