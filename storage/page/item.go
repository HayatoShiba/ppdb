/*
`item` is interchangeably used with `heap tuple` / `index tuple` ...

item-related interface is
- GetItem(PagePtr, SlotIndex): gets item from page. the location of the item is calculated from SlotIndex's Slot
- AddItem(PagePtr, ItemPtr, SlotIndex): adds item to the page. if the page does not have enough space, return error.
*/
package page

// ItemPtr points to item within page
// item length is variable
type ItemPtr []byte

// itemOffset is the byte offset of the item within page
// itemOffset is used when extracting item offset from Slot and assigning it to variable
// see Slot type for more details
type itemOffset uint16

// itemSize is the size of the item
// itemSize is used when extracting item size from Slot and assigning it to variable
// see Slot type for more details
type itemSize uint16
