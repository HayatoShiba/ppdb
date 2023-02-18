/*
clog bitmap

The state of each transaction is represented with 2 bits in clog file.
So the page looks just like the array of 2bits.
The location of the transaction (byte offset within page and bit offset within a byte) can
be calculated from transaction id.
*/
package clog

import (
	"github.com/HayatoShiba/ppdb/storage/page"
	"github.com/HayatoShiba/ppdb/transaction/txid"
)

// state is the state of each transaction
// this is represented with 2bits
// see https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/access/clog.h#L25-L30
type state int

const (
	// 0 indicates the transaction is in progress. so when initialization of page,
	// all transactions in page is treated as in-progress.
	stateInProgress state = 0x00
	stateCommitted  state = 0x01
	stateAborted    state = 0x02
)

const (
	// 2bits per transaction. see State
	clogBits = 2
	// clogNumPerByte is the number of clog per byte
	// 2bits * 4 = 1 byte
	clogNumPerByte = 4
	// clogNumPerPage is the number of clog per page
	// clogNumPerByte * pageSize(byte) = the number of clog in page
	clogNumPerPage = page.PageSize * clogNumPerByte
)

// getPageIDFromTxID returns page id calculated from transaction id
func getPageIDFromTxID(txID txid.TxID) page.PageID {
	return page.PageID(txID / clogNumPerPage)
}

// getByteOffsetFromTxID returns byte offset within page calculated from transaction id
func getByteOffsetFromTxID(txID txid.TxID) int {
	clogNumInPage := int(txID % clogNumPerPage)
	return clogNumInPage / clogNumPerByte
}

// getBitOffsetFromTxID returns bit offset within byte calculated from transaction id
// this offset can be 0-3
func getBitOffsetFromTxID(txID txid.TxID) int {
	clogNumInByte := int(txID % clogNumPerByte)
	return clogNumInByte * clogBits
}

// getState gets tx state
// see https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/backend/access/transam/clog.c#L638
func getState(data byte, txID txid.TxID) state {
	bitOffset := getBitOffsetFromTxID(txID)
	// shift the bits we want to the lowest position
	st := data >> (6 - bitOffset)
	// then use & mask to get the lowest position
	mask := byte((1 << 2) - 1)
	st = st & mask
	return state(st)
}

// getUpdatedState updates data with new status and returns it
func getUpdatedState(data byte, txID txid.TxID, st state) byte {
	bitOffset := getBitOffsetFromTxID(txID)
	// update the bits we want to update to 00. other bits are not changed
	mask := byte((0x03 << (6 - bitOffset)))
	data = data & ^mask
	// then use | mask to update the bits
	return data | (byte(st) << (6 - bitOffset))
}
