package disk

import "testing"

func TestingNewManager(t *testing.T) (*Manager, error) {
	baseDir = t.TempDir()
	return NewManager()
}
