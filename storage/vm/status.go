package vm

const (
	// the vm bits corresponding to the page has been initialized
	StatusInitialized uint8 = 0x00
	// all tuples within the page is visible to any transaction
	StatusAllVisible uint8 = 0x01
	// all tuples within the page is frozen
	StatusAllFrozen uint8 = 0x02
)

// IsAllVisible checks whether the status indicates all-visible or not
func IsAllVisible(flags uint8) bool {
	return (flags & StatusAllVisible) != 0
}

// IsAllFrozen checks whether the status indicates all-frozen or not
func IsAllFrozen(flags uint8) bool {
	return (flags & StatusAllFrozen) != 0
}
