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

func testBackend(kvmap KVMap, t *testing.T) {
	if kvmap.MapName() != Sync {
		keyCount := kvmap.GetKeyCount()
		if keyCount != 0 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", kvmap.MapName(), keyCount)
		}
	} else {
		keyCount := kvmap.GetKeyCount()
		if keyCount != -1 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", kvmap.MapName(), keyCount)
		}
	}
	testValue := []byte("testvalue")
	kvmap.SetKey("testKey", testValue)
	returnedValue, found := kvmap.GetKey("testKey")
	if !found {
		t.Errorf("Backend %s returned found!=true for existing key", kvmap.MapName())
	}
	if bytes.Compare(returnedValue, testValue) != 0 {
		t.Errorf("Backend %s returned %s, expected %s", kvmap.MapName(), returnedValue, testValue)
	}
	_, found = kvmap.GetKey("invalidkey")
	if found {
		t.Errorf("Backend %s returned found=true for invalid key", kvmap.MapName())
	}
	if kvmap.MapName() != Sync {
		keyCount := kvmap.GetKeyCount()
		if keyCount != 1 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", kvmap.MapName(), keyCount)
		}
	} else {
		keyCount := kvmap.GetKeyCount()
		if keyCount != -1 {
			t.Errorf("Backend %s returned invalid key count for empty db: %d", kvmap.MapName(), keyCount)
		}
	}
	testValue1 := []byte("testvalue1")
	kvmap.SetKey("testkey1", testValue1)
	returnedValue, found = kvmap.GetKey("testkey1")
	if !found {
		t.Errorf("Backend %s returned found!=true for existing key", kvmap.MapName())
	}
	if bytes.Compare(returnedValue, testValue1) != 0 {
		t.Errorf("Backend %s returned %s, expected %s", kvmap.MapName(), returnedValue, testValue1)
	}
	returnedValue, found = kvmap.GetKey("testKey")
	if !found {
		t.Errorf("Backend %s returned found!=true for original key after adding a new one", kvmap.MapName())
	}
	if bytes.Compare(returnedValue, testValue) != 0 {
		t.Errorf("Backend %s returned %s for the original key after another key was added, expected %s", kvmap.MapName(), returnedValue, testValue)
	}
	newTestValue := []byte("testvalue2")
	kvmap.SetKey("testKey", newTestValue)
	returnedValue, found = kvmap.GetKey("testKey")
	if !found {
		t.Errorf("Backend %s returned found!=true for original key after modifying", kvmap.MapName())
	}
	if bytes.Compare(returnedValue, newTestValue) != 0 {
		t.Errorf("Backend %s returned %s for the original key after another key was added, expected %s", kvmap.MapName(), returnedValue, newTestValue)
	}
}

func testItems(kvmap KVMap, t *testing.T) {
	kvmap.SetKey("testkey1", []byte("testvalue1"))
	kvmap.SetKey("testkey2", []byte("testvalue2"))
	incomingChannel := make(chan KVPair)
	var wg sync.WaitGroup
	var keyCount int
	go func() {
		wg.Add(1)
		for range incomingChannel {
			keyCount++
		}
		wg.Done()
	}()
	kvmap.Items(incomingChannel)
	wg.Wait()
}

func testDelete(kvmap KVMap, t *testing.T) {
	var returnedValue []byte
	var found bool
	newTestValue := []byte("testvalue2")
	kvmap.SetKey("testKey", newTestValue)
	_, found = kvmap.GetKey("testKey")
	if !found {
		t.Errorf("kvmap %s init failed: setting key had no effect", kvmap.MapName())
	}
	kvmap.DeleteKey("testKey")
	returnedValue, found = kvmap.GetKey("testKey")
	if found {
		t.Errorf("Delete %s did not delete key: value is %s", kvmap.MapName(), returnedValue)
	}
	kvmap.DeleteKey("testkey")
}

func benchmarkMap(b *testing.B, kvmap KVMap) {
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
	kvmap := GetBackend(Basic)
	benchmarkMap(b, kvmap)
}

func BenchmarkSyncKvMap(b *testing.B) {
	kvmap := GetBackend(Sync)
	benchmarkMap(b, kvmap)
}

func BenchmarkShardedKvMap(b *testing.B) {
	kvmap := GetBackend(Sharded)
	benchmarkMap(b, kvmap)
}

func TestBasicKvMap(t *testing.T) {
	kvmap := GetBackend(Basic)
	testBackend(kvmap, t)
}

func TestSyncKvMap(t *testing.T) {
	kvmap := GetBackend(Sync)
	testBackend(kvmap, t)
}

func TestRaceKvMap(t *testing.T) {
	kvmap := GetBackend(Race)
	testBackend(kvmap, t)
}

func TestShardedKvMap(t *testing.T) {
	kvmap := GetBackend(Sharded)
	testBackend(kvmap, t)
}

func TestBasicKvMapItems(t *testing.T) {
	kvmap := GetBackend(Basic)
	testItems(kvmap, t)
}

func TestSyncKvMapItems(t *testing.T) {
	kvmap := GetBackend(Sync)
	testItems(kvmap, t)
}

func TestRaceKvMapItems(t *testing.T) {
	kvmap := GetBackend(Race)
	testItems(kvmap, t)
}

func TestShardedKvMapItems(t *testing.T) {
	kvmap := GetBackend(Sharded)
	testItems(kvmap, t)
}

func TestBasicKvMapDelete(t *testing.T) {
	kvmap := GetBackend(Basic)
	testDelete(kvmap, t)
}

func TestSyncKvMapDelete(t *testing.T) {
	kvmap := GetBackend(Sync)
	testDelete(kvmap, t)
}

func TestRaceKvMapDelete(t *testing.T) {
	kvmap := GetBackend(Race)
	testDelete(kvmap, t)
}

func TestShardedKvMapDelete(t *testing.T) {
	kvmap := GetBackend(Sharded)
	testDelete(kvmap, t)
}

func TestFileStorageBackend(t *testing.T) {
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	kvmap := GetBackend(FileBacked)
	testBackend(kvmap, t)
	kvmap2 := GetBackend(FileBacked)
	keyCount := kvmap2.GetKeyCount()
	if keyCount != 2 {
		t.Errorf("A new file backed storage did not load old data. Got %d", keyCount)
	}
}

func TestCachedFileStorageBackend(t *testing.T) {
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	kvmap := GetBackend(CachedFileBacked)
	testBackend(kvmap, t)
	time.Sleep(100 * time.Millisecond)
	kvmap2 := GetBackend(CachedFileBacked)
	keyCount := kvmap2.GetKeyCount()
	if keyCount != 2 {
		t.Errorf("A new cached file backed storage did not load old data. Got %d", keyCount)
	}
}

func TestFileStorageBackendDelete(t *testing.T) {
	kvmap := GetBackend(FileBacked)
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	testDelete(kvmap, t)
	kvmap2 := GetBackend(CachedFileBacked)
	keyCount := kvmap2.GetKeyCount()
	if keyCount != 0 {
		t.Error("A new file backed storage did load deleted data")
	}
}

func TestCachedFileStorageBackendDelete(t *testing.T) {
	hasher := sha256.New()
	hasher.Write([]byte(string(time.Now().String())))
	dataDirectory := fmt.Sprintf("%x", hasher.Sum(nil))
	os.Setenv("DATA_DIRECTORY", dataDirectory)
	defer os.RemoveAll(dataDirectory)
	kvmap := GetBackend(CachedFileBacked)
	testDelete(kvmap, t)
	time.Sleep(100 * time.Millisecond)
	kvmap2 := GetBackend(CachedFileBacked)
	keyCount := kvmap2.GetKeyCount()
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
	defer os.RemoveAll(dataDirectory)
	kvmap := GetBackend(FileBacked)
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
	defer os.RemoveAll(dataDirectory)
	kvmap := GetBackend(CachedFileBacked)
	err := os.RemoveAll(dataDirectory)
	if err != nil {
		t.Errorf("Removing data dir failed with %s", err)
	}
	keyCount := kvmap.GetKeyCount()
	if keyCount != 0 {
		t.Error("keycount did not fail with 0")
	}
}
