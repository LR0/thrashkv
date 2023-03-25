package util

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

// Arena 粗糙的内存分配器，存储序列化的值，需要是lock-free的
type Arena struct {
	waterLevel uint32 // 所有操作都应该原子化
	shouldGrow bool
	buf        []byte
}

// newArena 传入初始大小
func newArena(n int64) *Arena {
	out := &Arena{
		waterLevel: 1,
		buf:        make([]byte, n),
	}
	return out
}

// 预先分配空间
func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.waterLevel, sz)

	if int(offset) > len(s.buf)-MaxNodeSize {

		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.waterLevel))
}

// putNode 分配指针堆的空间
func (s *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (s *Arena) putVal(val ValueStruct) uint32 {
	l := val.EncodedSize()
	offset := s.allocate(l)
	val.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	l := uint32(len(key))
	offset := s.allocate(l)
	buf := s.buf[s.waterLevel:offset]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (res ValueStruct) {
	res.DecodeValue(s.buf[offset : offset+size])
	return
}

func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
