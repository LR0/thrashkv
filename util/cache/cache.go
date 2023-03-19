package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	door      *BloomFilter
	c         *cmSketch
	t         int32
	threshold int32
	data      map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

func NewCache(size int) *Cache {
	const lruPct = 1 // 规定window缓存占百分之一
	lruSz := (lruPct * size) / 100
	if lruSz < 1 {
		lruSz = 1
	}

	// 计算 LFU 部分的缓存容量
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))

	if slruSz < 1 {
		slruSz = 1
	}

	//LFU 分为两部分，stageOne 部分占比20%
	slruO := int(0.2 * float64(slruSz))

	if slruO < 1 {
		slruO = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		door: newFilter(size, 0.01), //布隆过滤器设置误差率为0.01
		c:    newCmSketch(int64(size)),
		data: data, //共用同一个 map 存储数据
	}
}

// 计算存储hash，和唯一性hash
func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

func (c *Cache) set(key, value interface{}) bool {
	// keyHash 用来快速定位，conflice 用来判断冲突
	keyHash, conflictHash := c.keyToHash(key)

	// 刚放进去的缓存都先放到 window lru 中，所以 stage = 0
	item := storeItem{
		stage:    STAGE_WINDOW,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	eitem, evicted := c.lru.add(item)

	if !evicted {
		return true
	}

	victim := c.slru.victim()
	if victim == nil {
		c.slru.add(eitem)
		return true
	}

	// 在 bf 中出现，说明访问频率 >= 2
	if !c.door.Allow(uint32(eitem.key)) {
		return true
	}

	// 估算 windowlru 和 LFU 中淘汰数据，历史访问频次
	// 访问频率高的，被认为更有资格留下来
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eitem.key)
	if ocount < vcount {
		return true
	}
	c.slru.add(eitem)
	return true
}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		c.c.Reset()
		c.door.reset()
		c.t = 0
	}

	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	// 唯一hash不同，说明实际上不是一个元素
	if item.conflict != conflictHash {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	c.door.Allow(uint32(keyHash))
	c.c.Increment(item.key)

	v := item.value

	if item.stage == STAGE_WINDOW {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}

	return v, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)
	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	delete(c.data, keyHash)
	return item.conflict, true
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
