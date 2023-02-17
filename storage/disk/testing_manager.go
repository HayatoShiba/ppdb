package disk

import "testing"

// TestingNewFileManager initializes disk manager with file storage.
func TestingNewFileManager(t *testing.T) (*Manager, error) {
	baseDir = t.TempDir()
	return NewManager()
}

// TestingNewManager initializes disk manager with buffer storage instead of file storage. This prevents unnecessary disk I/O.
func TestingNewBufferManager() (*Manager, error) {
	return &Manager{newBufferOpener()}, nil
}
