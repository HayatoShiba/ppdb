/*
Simple implementation of heap tuple.
Postgres has a lot, like null bitmap, various hint bits...
ppdb currently doesn't have plan to implement data type, so omit many fields.
Tuple in ppdb has only three fields currently.

- xmin: in what transaction the tuple is inserted
- xmax: in what transaction the tuple is updated or deleted
- ctid: the tuple's tid / or the new tuple's tid after the tuple is updated
  - the tuples are linked through this field like linked list

- infomask: various information bit. for example a bit indicates xmin has been frozen

xmin, xmax, ctid, and infomask are necessary for MVCC.
see comment at /transaction/manager.go
*/
package tuple

import (
	"encoding/binary"

	"github.com/HayatoShiba/ppdb/transaction/txid"
)

// Tuple is in-memory structure for tuple
type Tuple struct {
	// size is the byte size of tuple
	// in ppdb, this is used when insert/update tuple
	// this is t_len in postgres HeapTupleData
	// size uint32

	// the location on disk
	// this is t_self in postgres HeapTupleData
	// self Tid

	// the relation of this tuple
	// this is t_tableOid in postgres HeapTupleData
	// rel common.Relation

	// tuple header information
	xmin txid.TxID
	xmax txid.TxID
	// this is pointer to the location of this tuple or new tuple
	// initialized with the location of this tuple and updated when this tuple updated,
	// this is t_ctid in postgres HeapTupleHeaderData
	ctid Tid

	// various information bit. for example a bit indicates xmin has been frozen
	infomask uint16

	// the pointer to on-disk tuple data
	// this is t_data in postgres HeapTupleData
	data []byte
}

/*
TupleByte is on-disk byte slice for tuple
tuple is

- tuple header: 16 byte
  - xmin: 4 byte
  - xmax: 4 byte
  - ctid: 8 byte
  - infomask: 2byte

- tuple data: 1 byte
*/
type TupleByte []byte

const (
	//  tuple header size is 16 byte
	tupleHeaderSize = 18
)

const (
	// the offset of xmin field in tuple is 0 byte
	xminOffset = 0
	// the offset of xmax field in tuple is 4 byte
	xmaxOffset = 4
	// the offset of ctid field in tuple is 8 byte
	ctidOffset = 8
	// the offset of infomask field in tuple is 16 byte
	infomaskOffset = 16
	// the offset of tuple data is 18 byte
	dataOffset = 18
)

// newTuple initializes tuple
func newTuple(xmin txid.TxID, ctid Tid, infomask uint16, data []byte) Tuple {
	return Tuple{
		xmin:     xmin,
		xmax:     txid.InvalidTxID,
		ctid:     ctid,
		infomask: infomask,
		data:     data,
	}
}

func (tup Tuple) size() int {
	// free spaceを探すときにサイズが必要
	return len(tup.data)
}

// marshalTuple marshals tuple
// this function is expected to be called when insert new tuple
func marshalTuple(tuple Tuple) TupleByte {
	b := make([]byte, 0, tupleHeaderSize+len(tuple.data))
	b = binary.LittleEndian.AppendUint32(b, uint32(tuple.xmin))
	b = binary.LittleEndian.AppendUint32(b, uint32(tuple.xmax))
	b = binary.LittleEndian.AppendUint64(b, marshalTid(tuple.ctid))
	b = binary.LittleEndian.AppendUint16(b, uint16(tuple.infomask))
	b = append(b, tuple.data...)
	return TupleByte(b)
}

// Xmin returns xmin
func (t TupleByte) Xmin() txid.TxID {
	b := t[xminOffset : xminOffset+4]
	xmin := binary.LittleEndian.Uint32(b)
	return txid.TxID(xmin)
}

// SetXmin sets xmin
func (t TupleByte) SetXmin(txID txid.TxID) {
	binary.LittleEndian.PutUint32(t[xminOffset:xminOffset+4], uint32(txID))
}

// Xmax returns xmax
func (t TupleByte) Xmax() txid.TxID {
	b := t[xmaxOffset : xmaxOffset+4]
	xmax := binary.LittleEndian.Uint32(b)
	return txid.TxID(xmax)
}

// SetXmax sets xmax
func (t TupleByte) SetXmax(txID txid.TxID) {
	binary.LittleEndian.PutUint32(t[xmaxOffset:xmaxOffset+4], uint32(txID))
}

// Ctid returns ctid
func (t TupleByte) Ctid() Tid {
	b := t[ctidOffset : ctidOffset+4]
	ctid := unmarshalTid(b)
	return Tid(ctid)
}

// SetXmax sets xmax
func (t TupleByte) SetCtid(ctid Tid) {
	b := marshalTid(ctid)
	binary.LittleEndian.PutUint64(t[ctidOffset:ctidOffset+8], b)
}

// infomask bits
// https://github.com/postgres/postgres/blob/75f49221c22286104f032827359783aa5f4e6646/src/include/access/htup_details.h#L203-L210
// I'm not sure why these numbers are used 0100, 0200, 0400. why 0300 skipped?
const (
	// xminCommitted indicates xmin has been committed
	// when tuple visibility is checked, clog has to be checked. it may result in disk IO
	// so this bit is set when clog is checked and the status is committed (probably...)
	xminCommitted uint16 = 0x01000
	// xminInvalid indicates xmin is invalid/aborted
	xminInvalid uint16 = 0x02000
	// xminFrozen indicates xmin has been frozen.
	// frozen transaction id (2) is not used anymore. when the tuple is frozen, this bit is used.
	// xmaxFrozen is not defined because `xmax is frozen` indicates that no transaction can view the tuple
	xminFrozen uint16 = 0x04000
	// xmaxCommitted indicates xmax is committed
	xmaxCommitted uint16 = 0x08000
	// xmaxInvalid indicates xmax is invalid/aborted
	xmaxInvalid uint16 = 0x1000
)

// infomask returns infomask field
func (t TupleByte) infomask() uint16 {
	b := t[infomaskOffset : infomaskOffset+2]
	infomask := binary.LittleEndian.Uint16(b)
	return infomask
}

// XminCommitted returns whether xmin has been committed
func (t TupleByte) XminCommitted() bool {
	return (t.infomask() & xminCommitted) != 0
}

// SetXminCommitted sets xmin is committed
func (t TupleByte) SetXminCommitted() {
	infomask := t.infomask() | xminCommitted
	binary.LittleEndian.PutUint16(t[infomaskOffset:infomaskOffset+2], uint16(infomask))
}

// XminInvalid returns whether xmin is invalid
func (t TupleByte) XminInvalid() bool {
	return (t.infomask() & xminInvalid) != 0
}

// SetXminInvalid sets xmin is invalid
func (t TupleByte) SetXminInvalid() {
	infomask := t.infomask() | xminInvalid
	binary.LittleEndian.PutUint16(t[infomaskOffset:infomaskOffset+2], uint16(infomask))
}

// XminFrozen returns whether xmin is frozen
func (t TupleByte) XminFrozen() bool {
	return (t.infomask() & xminFrozen) != 0
}

// SetXminFrozen sets xmin is frozen
func (t TupleByte) SetXminFrozen() {
	infomask := t.infomask() | xminFrozen
	binary.LittleEndian.PutUint16(t[infomaskOffset:infomaskOffset+2], uint16(infomask))
}

// XmaxCommitted returns whether xmax is committed
func (t TupleByte) XmaxCommitted() bool {
	return (t.infomask() & xmaxCommitted) != 0
}

// SetXmaxCommitted sets xmax is committed
func (t TupleByte) SetXmaxCommitted() {
	infomask := t.infomask() | xmaxCommitted
	binary.LittleEndian.PutUint16(t[infomaskOffset:infomaskOffset+2], uint16(infomask))
}

// XmaxInvalid returns whether xmax is invalid
func (t TupleByte) XmaxInvalid() bool {
	return (t.infomask() & xmaxInvalid) != 0
}

// SetXmaxInvalid sets xmax is invalid
func (t TupleByte) SetXmaxInvalid() {
	infomask := t.infomask() | xmaxInvalid
	binary.LittleEndian.PutUint16(t[infomaskOffset:infomaskOffset+2], uint16(infomask))
}
