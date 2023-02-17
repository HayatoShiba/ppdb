package disk

import (
	"fmt"
	"path/filepath"

	"github.com/HayatoShiba/ppdb/common"
)

// relation has main table file, fsm file, vm file and these are identified with fork number.
// see the links below
// https://github.com/postgres/postgres/blob/b0a55e43299c4ea2a9a8c757f9c26352407d0ccc/src/backend/storage/smgr/README#L37-L52
// https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/include/common/relpath.h#L39-L60
type ForkNumber int

const (
	// ForkNumberMain is fork number of main(table)
	// basically this fork is accessed
	ForkNumberMain ForkNumber = iota
	// ForkNumberFSM is fork number of free space map
	ForkNumberFSM
	// ForkNumberVM is fork number of visibility map
	ForkNumberVM
)

// maxForkNum is for how many fork number exists
const maxForkNum = ForkNumberVM

// forkFilePathSuffix is defined for file path
var forkFilePathSuffix = []string{"main", "fsm", "vm"}

// getRelationForkFilePath returns file path under base directory
// the path of each relation fork file in ppdb is described below
// - main table file: /base/database/tableOid
// - fsm file:  /base/database/tableOid_fsm
// - vm file: /base/database/tableOid_vm
// see https://github.com/postgres/postgres/blob/a448e49bcbe40fb72e1ed85af910dd216d45bad8/src/common/relpath.c#L141
func getRelationForkFilePath(rel common.Relation, forkNumber ForkNumber) string {
	if forkNumber == ForkNumberMain {
		return filepath.Join(baseDir, fmt.Sprintf("%d", rel))
	}
	return filepath.Join(baseDir, fmt.Sprintf("%d_%s", rel, forkFilePathSuffix[forkNumber]))
}
