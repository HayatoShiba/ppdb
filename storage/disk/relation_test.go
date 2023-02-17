package disk

import (
	"path/filepath"
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/stretchr/testify/assert"
)

func TestGetRelationForkFilePath(t *testing.T) {
	tests := []struct {
		name     string
		forkNum  ForkNumber
		expected string
	}{
		{
			name:     "get main table path",
			forkNum:  ForkNumberMain,
			expected: filepath.Join(baseDir, "1"),
		},
		{
			name:     "get fsm table path",
			forkNum:  ForkNumberFSM,
			expected: filepath.Join(baseDir, "1_fsm"),
		},
		{
			name:     "get vm table path",
			forkNum:  ForkNumberVM,
			expected: filepath.Join(baseDir, "1_vm"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getRelationForkFilePath(common.Relation(1), tt.forkNum)
			assert.Equal(t, tt.expected, got)
		})
	}
}
