package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	baseDir = t.TempDir()
	_, err := NewManager()
	assert.Nil(t, err)
}
