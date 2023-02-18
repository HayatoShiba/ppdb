package clog

import "testing"

func TestingNewDiskManager(t *testing.T) (*diskManager, error) {
	dir = t.TempDir()
	return newDiskManager()
}
