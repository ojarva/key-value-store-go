package storage

import (
	"bytes"
	"testing"
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
