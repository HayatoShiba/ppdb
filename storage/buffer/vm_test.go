package buffer

import (
	"testing"

	"github.com/HayatoShiba/ppdb/common"
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/storage/vm"
	"github.com/stretchr/testify/assert"
)

func TestUpdateVMStatus(t *testing.T) {
	m, err := TestingNewManager()
	assert.Nil(t, err)

	rel := common.Relation(1)
	pageID := page.PageID(10)
	flags := vm.StatusAllVisible

	err = m.UpdateVMStatus(rel, pageID, flags)
	assert.Nil(t, err)

	got, err := m.GetVMStatus(rel, pageID)
	assert.Nil(t, err)

	assert.Equal(t, flags, got)
}
