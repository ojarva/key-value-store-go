package storage

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func testBackend(kvmap KVMap, backendName string, t *testing.T) {
	kvmap.Init()
	if backendName != "sync" {
		keyCount := kvmap.GetKeyCount()
		if keyCount != 0 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", backendName, keyCount)
		}
	} else {
		keyCount := kvmap.GetKeyCount()
		if keyCount != -1 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", backendName, keyCount)
		}
	}
	testValue := []byte("testvalue")
	kvmap.SetKey("testKey", testValue)
	returnedValue, found := kvmap.GetKey("testKey")
	if !found {
		t.Errorf("Backend %s returned found!=true for existing key", backendName)
	}
	if bytes.Compare(returnedValue, testValue) != 0 {
		t.Errorf("Backend %s returned %s, expected %s", backendName, returnedValue, testValue)
	}
	_, found = kvmap.GetKey("invalidkey")
	if found {
		t.Errorf("Backend %s returned found=true for invalid key", backendName)
	}
	if backendName != "sync" {
		keyCount := kvmap.GetKeyCount()
		if keyCount != 1 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", backendName, keyCount)
		}
	} else {
		keyCount := kvmap.GetKeyCount()
		if keyCount != -1 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", backendName, keyCount)
		}
	}
	testValue1 := []byte("testvalue1")
	kvmap.SetKey("testkey1", testValue1)
	returnedValue, found = kvmap.GetKey("testkey1")
	if !found {
		t.Errorf("Backend %s returned found!=true for existing key", backendName)
	}
	if bytes.Compare(returnedValue, testValue1) != 0 {
		t.Errorf("Backend %s returned %s, expected %s", backendName, returnedValue, testValue1)
	}
	returnedValue, found = kvmap.GetKey("testKey")
	if !found {
		t.Errorf("Backend %s returned found!=true for original key after adding a new one", backendName)
	}
	if bytes.Compare(returnedValue, testValue) != 0 {
		t.Errorf("Backend %s returned %s for the original key after another key was added, expected %s", backendName, returnedValue, testValue)
	}
	newTestValue := []byte("testvalue2")
	kvmap.SetKey("testKey", newTestValue)
	returnedValue, found = kvmap.GetKey("testKey")
	if !found {
		t.Errorf("Backend %s returned found!=true for original key after modifying", backendName)
	}
	if bytes.Compare(returnedValue, newTestValue) != 0 {
		t.Errorf("Backend %s returned %s for the original key after another key was added, expected %s", backendName, returnedValue, newTestValue)
	}

}

func testDelete(kvmap KVMap, backendName string, t *testing.T) {
	kvmap.Init()
	var returnedValue []byte
	var found bool
	newTestValue := []byte("testvalue2")
	kvmap.SetKey("testKey", newTestValue)
	_, found = kvmap.GetKey("testKey")
	if !found {
		t.Errorf("kvmap %s init failed: setting key had no effect", backendName)
	}
	kvmap.DeleteKey("testKey")
	returnedValue, found = kvmap.GetKey("testKey")
	if found {
		t.Errorf("Delete %s did not delete key: value is %s", backendName, returnedValue)
	}
	kvmap.DeleteKey("testkey")
}

func benchmarkMap(b *testing.B, kvmap KVMap) {
	kvmap.Init()
	b.ResetTimer()
	value := []byte("my test value")
	wg := &sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		go func() {
			wg.Add(1)
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key%d", rand.Int())
				kvmap.SetKey(key, value)
				kvmap.GetKey(key)
				kvmap.SetKey(key, value)
				kvmap.GetKey(key + "invalid")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkBasicKvMap(b *testing.B) {
	kvmap := &BasicKvMap{}
	benchmarkMap(b, kvmap)
}

func BenchmarkSyncKvMap(b *testing.B) {
	kvmap := &SyncKvMap{}
	benchmarkMap(b, kvmap)
}

func BenchmarkShardedKvMap(b *testing.B) {
	kvmap := &ShardedKvMap{}
	benchmarkMap(b, kvmap)
}

func TestBasicKvMap(t *testing.T) {
	kvmap := &BasicKvMap{}
	testBackend(kvmap, "basic", t)
}

func TestSyncKvMap(t *testing.T) {
	kvmap := &SyncKvMap{}
	testBackend(kvmap, "sync", t)
}

func TestRaceKvMap(t *testing.T) {
	kvmap := &RaceKvMap{}
	testBackend(kvmap, "race", t)
}

func TestShardedKvMap(t *testing.T) {
	kvmap := &ShardedKvMap{}
	testBackend(kvmap, "sharded", t)
}

func TestBasicKvMapDelete(t *testing.T) {
	kvmap := &BasicKvMap{}
	testDelete(kvmap, "basic", t)
}

func TestSyncKvMapDelete(t *testing.T) {
	kvmap := &SyncKvMap{}
	testDelete(kvmap, "sync", t)
}

func TestRaceKvMapDelete(t *testing.T) {
	kvmap := &RaceKvMap{}
	testDelete(kvmap, "race", t)
}

func TestShardedKvMapDelete(t *testing.T) {
	kvmap := &ShardedKvMap{}
	testDelete(kvmap, "sharded", t)
}

func TestFileStorageBackend(t *testing.T) {
	kvmap := &FileBackedStorage{}
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	testBackend(kvmap, "filebacked", t)
	kvmap2 := &CachedFileBackedStorage{}
	kvmap2.Init()
	keyCount := kvmap.GetKeyCount()
	if keyCount != 2 {
		t.Error("A new file backed storage did not load old data")
	}
}

func TestCachedFileStorageBackend(t *testing.T) {
	kvmap := &CachedFileBackedStorage{}
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	testBackend(kvmap, "filebacked", t)
	kvmap2 := &CachedFileBackedStorage{}
	kvmap2.Init()
	keyCount := kvmap.GetKeyCount()
	if keyCount != 2 {
		t.Error("A new file backed storage did not load old data")
	}
}

func TestFileStorageBackendDelete(t *testing.T) {
	kvmap := &FileBackedStorage{}
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	testDelete(kvmap, "filebacked", t)
	kvmap2 := &CachedFileBackedStorage{}
	kvmap2.Init()
	keyCount := kvmap.GetKeyCount()
	if keyCount != 0 {
		t.Error("A new file backed storage did load deleted data")
	}
}

func TestCachedFileStorageBackendDelete(t *testing.T) {
	kvmap := &CachedFileBackedStorage{}
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	testDelete(kvmap, "filebacked", t)
	kvmap2 := &CachedFileBackedStorage{}
	kvmap2.Init()
	keyCount := kvmap.GetKeyCount()
	if keyCount != 0 {
		t.Error("A new file backed storage did not load deleted data")
	}
}

func TestGetDataDir(t *testing.T) {
	os.Unsetenv("DATA_DIRECTORY")
	dd := getDataDir()
	if dd != "datadir" {
		t.Error("getDataDir returned incorrect dir")
	}
	os.Setenv("DATA_DIRECTORY", "/tmp/foo/")
	dd = getDataDir()
	if dd != "/tmp/foo" {
		t.Error("getDataDir returned incorrect dir")
	}
}

func TestFileBackedStorageGetKeyCount(t *testing.T) {
	// Normal operations alredy tested in normal tests -> test non-existing dir
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	kvmap := &FileBackedStorage{}
	kvmap.Init()
	err := os.RemoveAll(dataDirectory)
	if err != nil {
		t.Errorf("Removing data dir failed with %s", err)
	}
	keyCount := kvmap.GetKeyCount()
	if keyCount != -1 {
		t.Error("keycount did not fail with -1")
	}
}

func TestCachedFileBackedStorageGetKeyCount(t *testing.T) {
	// Normal operations alredy tested in normal tests -> test non-existing dir
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	kvmap := &CachedFileBackedStorage{}
	kvmap.Init()
	err := os.RemoveAll(dataDirectory)
	if err != nil {
		t.Errorf("Removing data dir failed with %s", err)
	}
	keyCount := kvmap.GetKeyCount()
	if keyCount != 0 {
		t.Error("keycount did not fail with 0")
	}
}
