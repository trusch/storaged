package storage

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

func benchmarkStoreAddValue(b *testing.B, store Storage, num int) {
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			store.AddValue("test", float64(i))
		}
	}
}

func benchmarkStoreDeleteRange(b *testing.B, store Storage, num int) {
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			store.AddValue("test", float64(i))
		}
		store.DeleteRange("test", time.Time{}, time.Now())
	}
}

func benchmarkStoreGetRange(b *testing.B, store Storage, num int) {
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			store.AddValue("test", float64(i))
		}
		ch, _ := store.GetRange("test", time.Time{}, time.Now())
		for _ = range ch {
		}
	}
}

func BenchmarkLevelDBStorageAddValue100(b *testing.B) {
	os.RemoveAll("./test-store.db")
	store, _ := NewMetaStorage("leveldb://test-store.db")
	benchmarkStoreAddValue(b, store, 100)
}

func BenchmarkBoltStorageAddValue100(b *testing.B) {
	os.RemoveAll("./test-store.db")
	store, _ := NewMetaStorage("bolt://test-store.db")
	benchmarkStoreAddValue(b, store, 100)
}

func BenchmarkMongoStorageAddValue100(b *testing.B) {
	cmd := exec.Command("mongo", "test-store", "--eval", "db.kv.drop(); db['ts/test/value'].drop(); db['ts/test'].drop();")
	cmd.Run()
	store, _ := NewMetaStorage("mongodb://localhost/test-store")
	benchmarkStoreAddValue(b, store, 100)
}

func BenchmarkLevelDBStorageDeleteRange100(b *testing.B) {
	os.RemoveAll("./test-store.db")
	store, _ := NewMetaStorage("leveldb://test-store.db")
	benchmarkStoreDeleteRange(b, store, 100)
}

func BenchmarkBoltStorageDeleteRange100(b *testing.B) {
	os.RemoveAll("./test-store.db")
	store, _ := NewMetaStorage("bolt://test-store.db")
	benchmarkStoreDeleteRange(b, store, 100)
}

func BenchmarkMongoStorageDeleteRange(b *testing.B) {
	cmd := exec.Command("mongo", "test-store", "--eval", "db.kv.drop(); db['ts/test/value'].drop(); db['ts/test'].drop();")
	cmd.Run()
	store, _ := NewMetaStorage("mongodb://localhost/test-store")
	benchmarkStoreDeleteRange(b, store, 100)
}

func BenchmarkLevelDBStorageGetRange100(b *testing.B) {
	os.RemoveAll("./test-store.db")
	store, _ := NewMetaStorage("leveldb://test-store.db")
	benchmarkStoreGetRange(b, store, 100)
}

func BenchmarkBoltStorageGetRange100(b *testing.B) {
	os.RemoveAll("./test-store.db")
	store, _ := NewMetaStorage("bolt://test-store.db")
	benchmarkStoreGetRange(b, store, 100)
}

func BenchmarkMongoStorageGetRange(b *testing.B) {
	cmd := exec.Command("mongo", "test-store", "--eval", "db.kv.drop(); db['ts/test/value'].drop(); db['ts/test'].drop();")
	cmd.Run()
	store, _ := NewMetaStorage("mongodb://localhost/test-store")
	benchmarkStoreGetRange(b, store, 100)
}
