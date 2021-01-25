package storage

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// BackendType defines which key-value map backend is used.
type BackendType int

//go:generate stringer -type=BackendType
const (
	// Basic is a thread-safe, locked, a single-map backend
	Basic BackendType = iota
	// Race is a non-thread-safe testing backend which uses no locking, causing data races/runtime errors for simultaneous reads/writes.
	Race
	// Sharded is a thread-safe and locked database which automatically shards keys to different maps and uses separate lock for each map.
	Sharded
	// Sync uses golang's built-in sync.Map, which is especially powerful for append-only usage.
	Sync
	// FileBacked stores all keys and values directly to filesystem, having great durability but very poor performance.
	FileBacked
	// CachedFileBacked keeps all keys and values in memory but a separate background thread stores keys to the disk.
	CachedFileBacked
)

// GetBackend returns initialized backend
func GetBackend(t BackendType) KVMap {
	var kvmap KVMap
	switch t {
	case Basic:
		kvmap = &basicKvMap{}
	case Race:
		kvmap = &raceKvMap{}
	case Sharded:
		kvmap = &shardedKvMap{}
	case Sync:
		kvmap = &syncKvMap{}
	case FileBacked:
		kvmap = &fileBackedStorage{}
	case CachedFileBacked:
		kvmap = &cachedFileBackedStorage{}
	default:
		panic(fmt.Sprintf("Unhandled kvmap type: %d", t))
	}
	kvmap.init()
	return kvmap
}

func getDataDir() string {
	path := os.Getenv("DATA_DIRECTORY")
	if path == "" {
		path = "datadir"
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	return path
}

// KVPair represents a single key-value pair from the backend.
type KVPair struct {
	Key   string
	Value []byte
}

// KVMap specifies storage backend interface
type KVMap interface {
	DeleteKey(key string)
	GetKey(key string) ([]byte, bool)
	GetKeyCount() int
	init()
	SetKey(key string, value []byte)
	Items(chan KVPair)
	MapName() BackendType
}

// raceKvMap is unsafe implementation without a proper locking.
type raceKvMap struct {
	values map[string][]byte
}

// init initializes the backend
func (d *raceKvMap) init() {
	d.values = make(map[string][]byte)
}

// SetKey stores a new key and associated value to the backend
func (d *raceKvMap) SetKey(key string, value []byte) {
	d.values[key] = value
}

// GetKey returns a value or error (nil, false) for specified key
func (d *raceKvMap) GetKey(key string) ([]byte, bool) {
	val, found := d.values[key]
	return val, found
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *raceKvMap) GetKeyCount() int {
	return len(d.values)
}

// DeleteKey removes key and associated value from storage
func (d *raceKvMap) DeleteKey(key string) {
	delete(d.values, key)
}

// Items iterates over all keys in the map. Performance-wise this is terrible.
func (d *raceKvMap) Items(outChan chan KVPair) {
	for key, value := range d.values {
		outChan <- KVPair{Key: key, Value: value}
	}
	close(outChan)
}

// BackendName returns the type of the current backend.
func (d *raceKvMap) MapName() BackendType {
	return Race
}

// basicKvMap implements a simple backend with a single shared RWMutex to coordinate concurrent writes and reads.
type basicKvMap struct {
	values map[string][]byte
	mutex  sync.RWMutex
}

// init initializes the backend
func (d *basicKvMap) init() {
	d.values = make(map[string][]byte)
}

// SetKey stores a new key and associated value to the backend
func (d *basicKvMap) SetKey(key string, value []byte) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.values[key] = value
}

// GetKey returns a value or error (nil, false) for specified key
func (d *basicKvMap) GetKey(key string) ([]byte, bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	val, found := d.values[key]
	return val, found
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *basicKvMap) GetKeyCount() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return len(d.values)
}

// DeleteKey removes key and associated value from storage
func (d *basicKvMap) DeleteKey(key string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.values, key)
}

// Items iterates over all keys in the map. Performance-wise this is terrible.
func (d *basicKvMap) Items(outChan chan KVPair) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	for key, value := range d.values {
		outChan <- KVPair{Key: key, Value: value}
	}
	close(outChan)
}

// BackendName returns the type of the current backend.
func (d *basicKvMap) MapName() BackendType {
	return Basic
}

// syncKvMap uses sync.Map which automatically handles concurrent access. sync.Map is append-only structure which does not perform well for repeating writes / delete/add/delete/add cycles.
type syncKvMap struct {
	values sync.Map
}

// init initializes the backend
func (d *syncKvMap) init() {
	d.values = sync.Map{}
}

// GetKey returns a value or error (nil, false) for specified key
func (d *syncKvMap) GetKey(key string) ([]byte, bool) {
	val, ok := d.values.Load(key)
	if ok {
		return val.([]byte), ok
	}
	return nil, ok
}

// SetKey stores a new key and associated value to the backend
func (d *syncKvMap) SetKey(key string, value []byte) {
	d.values.Store(key, value)
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *syncKvMap) GetKeyCount() int {
	return -1
}

// DeleteKey removes key and associated value from storage
func (d *syncKvMap) DeleteKey(key string) {
	d.values.Delete(key)
}

// Items iterates over all keys in the map. Performance-wise this is terrible.
func (d *syncKvMap) Items(outChan chan KVPair) {
	sender := func(key interface{}, value interface{}) bool {
		outChan <- KVPair{Key: key.(string), Value: value.([]byte)}
		return true
	}
	d.values.Range(sender)
	close(outChan)
}

// BackendName returns the type of the current backend.
func (d *syncKvMap) MapName() BackendType {
	return Sync
}

// shardedKvMap implements a backend with separate locks for different shards. Sharded locking should reduce lock contention on a busy database, especially if some keys are extremely busy and keys in different shards are occasionally accessed.
type shardedKvMap struct {
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

// init initializes the backend
func (d *shardedKvMap) init() {
	d.mutexes = make([]sync.RWMutex, 16)
	d.values = make(map[uint8]map[string][]byte)
	var i uint8
	for i = 0; i < 16; i++ {
		d.values[i] = make(map[string][]byte)
	}
}

// GetKey returns a value or error (nil, false) for specified key
func (d *shardedKvMap) GetKey(key string) ([]byte, bool) {
	shard := getShard(key)
	d.mutexes[shard].RLock()
	defer d.mutexes[shard].RUnlock()
	val, found := d.values[shard][key]
	return val, found
}

// SetKey stores a new key and associated value to the backend
func (d *shardedKvMap) SetKey(key string, value []byte) {
	shard := getShard(key)
	d.mutexes[shard].Lock()
	defer d.mutexes[shard].Unlock()
	d.values[shard][key] = value
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *shardedKvMap) GetKeyCount() int {
	var i uint8
	var a int
	for i = 0; i < 16; i++ {
		d.mutexes[i].RLock()
		a += len(d.values[i])
		d.mutexes[i].RUnlock()
	}
	return a
}

// DeleteKey removes key and associated value from storage
func (d *shardedKvMap) DeleteKey(key string) {
	shard := getShard(key)
	d.mutexes[shard].Lock()
	defer d.mutexes[shard].Unlock()
	delete(d.values[shard], key)
}

// Items iterates over all keys in the map. Performance-wise this is terrible.
func (d *shardedKvMap) Items(outChan chan KVPair) {
	var i uint8
	for i = 0; i < 16; i++ {
		d.mutexes[i].RLock()
		for key, value := range d.values[i] {
			outChan <- KVPair{Key: key, Value: value}
		}
		d.mutexes[i].RUnlock()
	}
	close(outChan)
}

// BackendName returns the type of the current backend.
func (d *shardedKvMap) MapName() BackendType {
	return Sharded
}

// fileBackedStorage uses files directly to store all the data. This is highly inefficient but very durable.
type fileBackedStorage struct {
	mutex         sync.RWMutex
	dataDirectory string
}

func getHashedFilename(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return fmt.Sprintf("%x.kvdata", hasher.Sum(nil))
}

// init initializes the backend
func (d *fileBackedStorage) init() {
	path := getDataDir()
	_, err := os.Stat(path)
	if err != nil {
		_ = os.Mkdir(path, os.ModeDir|0700)
	}
	fileInfo, err := os.Stat(path)
	if !fileInfo.IsDir() {
		log.Fatalf("%s is not a directory", path)
		os.Exit(6)
	}
	d.dataDirectory = path
}

// GetKey returns a value or error (nil, false) for specified key
func (d *fileBackedStorage) GetKey(key string) ([]byte, bool) {
	filename := d.dataDirectory + "/" + getHashedFilename(key)
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, false
	}
	return data, true
}

// SetKey stores a new key and associated value to the backend
func (d *fileBackedStorage) SetKey(key string, value []byte) {
	filename := d.dataDirectory + "/" + getHashedFilename(key)
	d.mutex.Lock()
	defer d.mutex.Unlock()
	ioutil.WriteFile(filename, value, 0600)
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *fileBackedStorage) GetKeyCount() int {
	files, err := ioutil.ReadDir(d.dataDirectory)
	if err != nil {
		return -1
	}
	count := 0
	for _, item := range files {
		if strings.HasSuffix(item.Name(), ".kvdata") {
			count++
		}
	}
	return count
}

// DeleteKey removes key and associated value from storage
func (d *fileBackedStorage) DeleteKey(key string) {
	filename := d.dataDirectory + "/" + getHashedFilename(key)
	d.mutex.Lock()
	defer d.mutex.Unlock()
	os.Remove(filename)
}

// Items iterates over all keys in the map. Not implemented for file-backed storage
func (d *fileBackedStorage) Items(outChan chan KVPair) {
	close(outChan)
}

// BackendName returns the type of the current backend.
func (d *fileBackedStorage) MapName() BackendType {
	return FileBacked
}

type fileSyncAction int

const (
	deleteAction fileSyncAction = iota
	setAction
)

type outFile struct {
	Data   KVPair
	Action fileSyncAction
}

// cachedFileBackedStorage keeps all the data in-memory but also asynchronously stores everything in files. When calling Init, all existing keys are loaded from the filesystem.
type cachedFileBackedStorage struct {
	values          map[string][]byte
	valuesMutex     sync.RWMutex
	writeoutChannel chan outFile
	dataDirectory   string
}

// init initializes the backend
func (d *cachedFileBackedStorage) init() {
	d.values = make(map[string][]byte)
	d.writeoutChannel = make(chan outFile, 1000)
	path := getDataDir()
	_, err := os.Stat(path)
	if err != nil {
		_ = os.Mkdir(path, os.ModeDir|0700)
	}
	fileInfo, err := os.Stat(path)
	if !fileInfo.IsDir() {
		log.Fatalf("%s is not a directory", path)
		os.Exit(6)
	}
	d.dataDirectory = path
	d.valuesMutex.Lock()
	defer d.valuesMutex.Unlock()
	files, _ := filepath.Glob(d.dataDirectory + "/*.kvdata")
	for _, filename := range files {
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Fatalf("Unable to read file %s: %s", filename, err)
			os.Exit(6)
		}
		fileParts := strings.Split(filename, "/")
		fileHash := fileParts[len(fileParts)-1]
		d.values[fileHash] = contents
	}
	go func() {
		var of outFile
		for of = range d.writeoutChannel {
			filename := d.dataDirectory + "/" + of.Data.Key
			switch of.Action {
			case setAction:
				ioutil.WriteFile(filename, of.Data.Value, 0600)
			case deleteAction:
				os.Remove(filename)
			}
		}
	}()
}

// GetKey returns a value or error (nil, false) for specified key
func (d *cachedFileBackedStorage) GetKey(key string) ([]byte, bool) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.RLock()
	defer d.valuesMutex.RUnlock()
	val, found := d.values[keyHash]
	return val, found
}

// SetKey stores a new key and associated value to the backend
func (d *cachedFileBackedStorage) SetKey(key string, value []byte) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.Lock()
	d.values[keyHash] = value
	d.valuesMutex.Unlock()
	d.writeoutChannel <- outFile{Data: KVPair{Key: keyHash, Value: value}, Action: setAction}
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *cachedFileBackedStorage) GetKeyCount() int {
	d.valuesMutex.RLock()
	defer d.valuesMutex.RUnlock()
	return len(d.values)
}

// DeleteKey removes key and associated value from storage
func (d *cachedFileBackedStorage) DeleteKey(key string) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.Lock()
	delete(d.values, keyHash)
	d.valuesMutex.Unlock()
	d.writeoutChannel <- outFile{Data: KVPair{Key: keyHash, Value: nil}, Action: deleteAction}
}

// Items iterates over all keys in the map. Not implemented for file-backed storage
func (d *cachedFileBackedStorage) Items(outChan chan KVPair) {
	close(outChan)
}

// BackendName returns the type of the current backend.
func (d *cachedFileBackedStorage) MapName() BackendType {
	return CachedFileBacked
}
