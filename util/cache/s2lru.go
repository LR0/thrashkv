package cache

import (
	"container/list"
	"fmt"
)

// 双端lru模拟lfu
// 1区为预备淘汰区,2区为热区
type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

// 会不会出现加一个已存在的元素
func (slru *segmentedLRU) add(newitem storeItem) {
	newitem.stage = STAGE_ONE

	if slru.stageOne.Len() < slru.stageOneCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	delete(slru.data, item.key)

	*item = newitem
	slru.data[newitem.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// 这里假设传入元素为stageOne中已有元素
	// 二级lru淘汰一个元素并与传入的一级元素交换

	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[bitem.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}

func (slru *segmentedLRU) victim() *storeItem {
	//如果 slru 的容量未满，不需要淘汰
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	// 如果已经满了，则需要从20%的区域淘汰数据，这里直接从尾部拿最后一个元素即可
	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
