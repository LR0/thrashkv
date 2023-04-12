package utils

import "encoding/binary"

type ValueStruct struct {
	Meta      byte // 元数据类型，区别数据来
	Value     []byte
	ExpiresAt uint64

	Version uint64 // This field is not serialized. Only for internal usage.
}

// 压缩后int的大小
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta占1byte
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// EncodeValue meta exp value
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}
