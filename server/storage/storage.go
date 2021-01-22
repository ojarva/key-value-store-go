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

// KVMap specifies storage backend interface
type KVMap interface {
	SetKey(key string, value []byte)
	GetKey(key string) ([]byte, bool)
	GetKeyCount() int
	Init()
}

// RaceKvMap is unsafe implementation without a proper locking.
type RaceKvMap struct {
	values map[string][]byte
}

// Init initializes the backend
func (d *RaceKvMap) Init() {
	(*d).values = make(map[string][]byte)
}

// SetKey stores a new key and associated value to the backend
func (d *RaceKvMap) SetKey(key string, value []byte) {
	(*d).values[key] = value
}

// GetKey returns a value or error (nil, false) for specified key
func (d *RaceKvMap) GetKey(key string) ([]byte, bool) {
	val, found := (*d).values[key]
	return val, found
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *RaceKvMap) GetKeyCount() int {
	return len((*d).values)
}

// BasicKvMap implements a simple backend with a single shared RWMutex to coordinate concurrent writes and reads.
type BasicKvMap struct {
	values map[string][]byte
	mutex  sync.RWMutex
}

// Init initializes the backend
func (d *BasicKvMap) Init() {
	(*d).values = make(map[string][]byte)
}

// SetKey stores a new key and associated value to the backend
func (d *BasicKvMap) SetKey(key string, value []byte) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	(*d).values[key] = value
}

// GetKey returns a value or error (nil, false) for specified key
func (d *BasicKvMap) GetKey(key string) ([]byte, bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	val, found := (*d).values[key]
	return val, found
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *BasicKvMap) GetKeyCount() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return len((*d).values)
}

// SyncKvMap uses sync.Map which automatically handles concurrent access. sync.Map is append-only structure which does not perform well for repeating writes / delete/add/delete/add cycles.
type SyncKvMap struct {
	values sync.Map
}

// Init initializes the backend
func (d *SyncKvMap) Init() {
	(*d).values = sync.Map{}
}

// GetKey returns a value or error (nil, false) for specified key
func (d *SyncKvMap) GetKey(key string) ([]byte, bool) {
	val, ok := (*d).values.Load(key)
	if ok {
		return val.([]byte), ok
	}
	return nil, ok
}

// SetKey stores a new key and associated value to the backend
func (d *SyncKvMap) SetKey(key string, value []byte) {
	(*d).values.Store(key, value)
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *SyncKvMap) GetKeyCount() int {
	return -1
}

// ShardedKvMap implements a backend with separate locks for different shards. Sharded locking should reduce lock contention on a busy database, especially if some keys are extremely busy and keys in different shards are occasionally accessed.
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

// Init initializes the backend
func (d *ShardedKvMap) Init() {
	(*d).mutexes = make([]sync.RWMutex, 16)
	(*d).values = make(map[uint8]map[string][]byte)
	var i uint8
	for i = 0; i < 16; i++ {
		(*d).values[i] = make(map[string][]byte)
	}
}

// GetKey returns a value or error (nil, false) for specified key
func (d *ShardedKvMap) GetKey(key string) ([]byte, bool) {
	shard := getShard(key)
	(*d).mutexes[shard].RLock()
	defer (*d).mutexes[shard].RUnlock()
	val, found := (*d).values[shard][key]
	return val, found
}

// SetKey stores a new key and associated value to the backend
func (d *ShardedKvMap) SetKey(key string, value []byte) {
	shard := getShard(key)
	(*d).mutexes[shard].Lock()
	defer (*d).mutexes[shard].Unlock()
	(*d).values[shard][key] = value
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
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

// FileBackedStorage uses files directly to store all the data. This is highly inefficient but very durable.
type FileBackedStorage struct {
	mutex         sync.RWMutex
	dataDirectory string
}

func getHashedFilename(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return fmt.Sprintf("%x.kvdata", hasher.Sum(nil))
}

// Init initializes the backend
func (d *FileBackedStorage) Init() {
	path := os.Getenv("DATA_DIRECTORY")
	if path == "" {
		path = "datadir"
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	_, err := os.Stat(path)
	if err != nil {
		_ = os.Mkdir(path, os.ModeDir|0700)
	}
	fileInfo, err := os.Stat(path)
	if !fileInfo.IsDir() {
		log.Fatalf("%s is not a directory", path)
		os.Exit(6)
	}
	(*d).dataDirectory = path
}

// GetKey returns a value or error (nil, false) for specified key
func (d *FileBackedStorage) GetKey(key string) ([]byte, bool) {
	filename := (*d).dataDirectory + "/" + getHashedFilename(key)
	(*d).mutex.RLock()
	defer (*d).mutex.RUnlock()
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, false
	}
	return data, true
}

// SetKey stores a new key and associated value to the backend
func (d *FileBackedStorage) SetKey(key string, value []byte) {
	filename := (*d).dataDirectory + "/" + getHashedFilename(key)
	(*d).mutex.Lock()
	defer (*d).mutex.Unlock()
	ioutil.WriteFile(filename, value, 0600)
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *FileBackedStorage) GetKeyCount() int {
	files, err := ioutil.ReadDir((*d).dataDirectory)
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

type outFile struct {
	Key   string
	Value []byte
}

// CachedFileBackedStorage keeps all the data in-memory but also asynchronously stores everything in files. When calling Init, all existing keys are loaded from the filesystem.
type CachedFileBackedStorage struct {
	values          map[string][]byte
	valuesMutex     sync.RWMutex
	writeoutChannel chan outFile
	dataDirectory   string
}

// Init initializes the backend
func (d *CachedFileBackedStorage) Init() {
	d.values = make(map[string][]byte)
	d.writeoutChannel = make(chan outFile, 1000)
	path := os.Getenv("DATA_DIRECTORY")
	if path == "" {
		path = "datadir"
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
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
			filename := d.dataDirectory + "/" + of.Key
			ioutil.WriteFile(filename, of.Value, 0600)
		}
	}()
}

// GetKey returns a value or error (nil, false) for specified key
func (d *CachedFileBackedStorage) GetKey(key string) ([]byte, bool) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.RLock()
	defer d.valuesMutex.RUnlock()
	val, found := d.values[keyHash]
	return val, found
}

// SetKey stores a new key and associated value to the backend
func (d *CachedFileBackedStorage) SetKey(key string, value []byte) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.Lock()
	d.values[keyHash] = value
	d.valuesMutex.Unlock()
	d.writeoutChannel <- outFile{Key: keyHash, Value: value}
}

// GetKeyCount returns number of keys or -1 if counting keys is not supported
func (d *CachedFileBackedStorage) GetKeyCount() int {
	d.valuesMutex.RLock()
	defer d.valuesMutex.RUnlock()
	return len((*d).values)
}
