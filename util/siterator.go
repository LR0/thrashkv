package util

type SkipListIterator struct {
	list *Skiplist
	n    *node
}

func (s *SkipListIterator) Rewind() {

}

func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

func (s *SkipListIterator) Value() ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

func (s *SkipListIterator) Valid() bool {
	return s.n != nil
}

func (s *SkipListIterator) ValueUint64() uint64 {
	return s.n.value
}

func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false)
}

func (s *SkipListIterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true)
}

func (s *SkipListIterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, false)
}

func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

func (s *SkipListIterator) SeekToLast() {
	s.n = s.list.findLast()
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:          s.Key(),
		Value:        nil,
		ExpiresAt:    0,
		Meta:         0,
		Version:      0,
		Offset:       0,
		Hlen:         0,
		ValThreshold: 0,
	}
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}
