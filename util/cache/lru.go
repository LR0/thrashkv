package cache

import (
	"container/list"
	"fmt"
)

const STAGE_WINDOW = 0

type windowLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	key      uint64
	value    interface{}
	stage    int // 两段式lru标志
	conflict uint64
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	if lru.list.Len() < lru.cap {
		lru.data[newitem.key] = lru.list.PushFront(&newitem)
		return storeItem{}, false
	}
	//如果 widow 部分容量已满，按照 lru 规则从尾部淘汰
	evictItem := lru.list.Back()
	item := evictItem.Value.(*storeItem)

	// 从 slice 中删除该条数据
	delete(lru.data, item.key)

	// 这里直接对 evictItem 和 *item 赋值，避免向runtime 再次申请空间
	// 直接替换了evictItem的*item
	eitem, *item = *item, newitem

	lru.data[item.key] = evictItem
	lru.list.MoveToFront(evictItem)
	return eitem, true
}

func (lru *windowLRU) get(v *list.Element) {
	lru.list.MoveToFront(v)
}

func (lru *windowLRU) String() string {
	var s string
	for e := lru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
