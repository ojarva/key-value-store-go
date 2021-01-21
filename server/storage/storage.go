package storage

import "sync"

type KVMap interface {
	SetKey(key string, value []byte)
	GetKey(key string) ([]byte, bool)
	GetKeyCount() int
	Init()
}

type RaceKvMap struct {
	values map[string][]byte
}

func (d *RaceKvMap) Init() {
	(*d).values = make(map[string][]byte)
}

func (d *RaceKvMap) SetKey(key string, value []byte) {
	(*d).values[key] = value
}

func (d *RaceKvMap) GetKey(key string) ([]byte, bool) {
	val, found := (*d).values[key]
	return val, found
}
func (d *RaceKvMap) GetKeyCount() int {
	return len((*d).values)
}

type BasicKvMap struct {
	values map[string][]byte
	mutex  sync.RWMutex
}

func (d *BasicKvMap) Init() {
	(*d).values = make(map[string][]byte)
}

func (d *BasicKvMap) SetKey(key string, value []byte) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	(*d).values[key] = value
}

func (d *BasicKvMap) GetKey(key string) ([]byte, bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	val, found := (*d).values[key]
	return val, found
}
func (d *BasicKvMap) GetKeyCount() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return len((*d).values)
}

type SyncKvMap struct {
	values sync.Map
}

func (d *SyncKvMap) Init() {
	(*d).values = sync.Map{}
}

func (d *SyncKvMap) GetKey(key string) ([]byte, bool) {
	val, ok := (*d).values.Load(key)
	if ok {
		return val.([]byte), ok
	}
	return nil, ok
}
func (d *SyncKvMap) SetKey(key string, value []byte) {
	(*d).values.Store(key, value)
}
func (d *SyncKvMap) GetKeyCount() int {
	return -1
}

type ShardedKvMap struct {
	values  map[uint8]map[string][]byte
	mutexes []sync.RWMutex
}

func getShard(key string) uint8 {
	s := 0
	for i := 0; i < len(key); i++ {
		s += int(key[i])
	}
	return uint8(s % 16)
}

func (d *ShardedKvMap) Init() {
	(*d).mutexes = make([]sync.RWMutex, 16)
	(*d).values = make(map[uint8]map[string][]byte)
	var i uint8
	for i = 0; i < 16; i++ {
		(*d).values[i] = make(map[string][]byte)
	}
}

func (d *ShardedKvMap) GetKey(key string) ([]byte, bool) {
	shard := getShard(key)
	(*d).mutexes[shard].RLock()
	defer (*d).mutexes[shard].RUnlock()
	val, found := (*d).values[shard][key]
	return val, found
}

func (d *ShardedKvMap) SetKey(key string, value []byte) {
	shard := getShard(key)
	(*d).mutexes[shard].Lock()
	defer (*d).mutexes[shard].Unlock()
	(*d).values[shard][key] = value
}

func (d *ShardedKvMap) GetKeyCount() int {
	var i uint8
	var a int
	for i = 0; i < 16; i++ {
		(*d).mutexes[i].RLock()
		a += len((*d).values[i])
		(*d).mutexes[i].RUnlock()
	}
	return a
}
