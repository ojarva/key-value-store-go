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

type FileBackedStorage struct {
	mutex         sync.RWMutex
	dataDirectory string
}

func getHashedFilename(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return fmt.Sprintf("%x.kvdata", hasher.Sum(nil))
}

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

func (d *FileBackedStorage) SetKey(key string, value []byte) {
	filename := (*d).dataDirectory + "/" + getHashedFilename(key)
	(*d).mutex.Lock()
	defer (*d).mutex.Unlock()
	ioutil.WriteFile(filename, value, 0600)
}

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

type CachedFileBackedStorage struct {
	values          map[string][]byte
	valuesMutex     sync.RWMutex
	writeoutChannel chan outFile
	dataDirectory   string
}

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

func (d *CachedFileBackedStorage) GetKey(key string) ([]byte, bool) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.RLock()
	defer d.valuesMutex.RUnlock()
	val, found := d.values[keyHash]
	return val, found
}

func (d *CachedFileBackedStorage) SetKey(key string, value []byte) {
	keyHash := getHashedFilename(key)
	d.valuesMutex.Lock()
	d.values[keyHash] = value
	d.valuesMutex.Unlock()
	d.writeoutChannel <- outFile{Key: keyHash, Value: value}
}

func (d *CachedFileBackedStorage) GetKeyCount() int {
	d.valuesMutex.RLock()
	defer d.valuesMutex.RUnlock()
	return len((*d).values)
}
