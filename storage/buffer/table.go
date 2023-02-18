/*
This is buffer table (just simple hash map)
In postgres, buffer table is extendable hash map?(probably?) and uses partitioning for performance optimization.
But ppdb currently defines buffer table as just simple global hash map with lock to the whole table,

for more details, see https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/backend/storage/buffer/buf_table.c#L3
*/
package buffer

import "sync"

// bufferTable is buffer table
type bufferTable struct {
	// mappint from buffer tag to buffer id
	table map[tag]BufferID
	// lock to the whole table
	sync.RWMutex
}
