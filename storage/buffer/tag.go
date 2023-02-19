package buffer

import (
	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/disk"
	"github.com/HayatoShiba/ppdb/storage/page"
)

// tag is buffer tag
// buffer tag must be sufficient to locate where the page is on disk
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/storage/buf_internals.h#L79-L98
type tag struct {
	// relation
	rel common.Relation
	// fork number
	forkNum disk.ForkNumber
	// page id
	pageID page.PageID
	// if valid is false, this descriptor hasn't been used so tag is invalid
	valid bool
}

// newBufferTag initializes buffer tag
func newTag(rel common.Relation, forkNum disk.ForkNumber, pageID page.PageID) *tag {
	return &tag{
		rel:     rel,
		forkNum: forkNum,
		pageID:  pageID,
	}
}
