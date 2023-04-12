package utils

import (
	"encoding/binary"
	"time"
)

//Entry _ 最外层写入的结构体
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64 // 为0代表永不过期

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int   // Length of the header.
	ValThreshold int64 // 超过界限，value为指针
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) IsDeletedOrExpired() bool {
	if e.Value == nil {
		return true
	}

	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

func (e *Entry) EncodedSize() uint32 {
	return uint32(len(e.Value) + sizeVarint(uint64(e.Meta)) + sizeVarint(e.ExpiresAt))
}

func (e *Entry) EstimateSize(threshold int) int {
	// TODO: 是否考虑 user meta?
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1 // Meta
	}
	return len(e.Key) + 12 + 1 // 12 for ValuePointer, 2 for meta.
}

type Header struct {
	KLen     uint32
	VLen     uint32
	ExpireAt uint64
	Meta     byte
}

// +------+------------+--------------+-----------+
// | Meta | Key Length | Value Length | ExpiresAt |
// +------+------------+--------------+-----------+
func (h Header) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpireAt)
	return index
}

func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
	h.ExpireAt, count = binary.Uvarint(buf[index:])
	return index + count
}
