package storage

import (
	"fmt"
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

//LevelDBStorage is an implementation for KeyValueStorage and TimeSeriesStorage
type LevelDBStorage struct {
	db *leveldb.DB
}

// NewLevelDBStorage creates a new storage instance
func NewLevelDBStorage(path string) (*LevelDBStorage, error) {
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile(path, o)
	if err != nil {
		return nil, err
	}
	store := &LevelDBStorage{db}
	return store, nil
}

// Put saves a value to the db
func (store *LevelDBStorage) Put(key string, value []byte) error {
	return store.db.Put([]byte("kv/"+key), value, nil)
}

// Get retrieves a value from db
func (store *LevelDBStorage) Get(key string) ([]byte, error) {
	bs, err := store.db.Get([]byte("kv/"+key), nil)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

// Delete drops an entry from db
func (store *LevelDBStorage) Delete(key string) error {
	return store.db.Delete([]byte("kv/"+key), nil)
}

// AddValue saves a value to the given timeseries
func (store *LevelDBStorage) AddValue(key string, value float64) error {
	keyBs := []byte(fmt.Sprintf("ts/%v%v", key, time.Now().UnixNano()))
	valBs := FloatToBytes(value)
	return store.db.Put(keyBs, valBs, nil)
}

// GetRange returns a channel which will give all values in a timerange
func (store *LevelDBStorage) GetRange(key string, from time.Time, to time.Time) (chan *TimeSeriesEntry, error) {
	ch := make(chan *TimeSeriesEntry, 64)
	startKey := []byte(fmt.Sprintf("ts/%v%v", key, from.UnixNano()))
	endKey := []byte(fmt.Sprintf("ts/%v%v", key, to.UnixNano()))
	iter := store.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
	if err := iter.Error(); err != nil {
		return nil, err
	}
	go func() {
		for iter.Next() {
			keyBs := iter.Key()
			valBs := iter.Value()
			nanos, err := strconv.ParseInt(string(keyBs[len(key)+3:]), 10, 64)
			if err != nil {
				continue
			}
			stamp := time.Unix(0, nanos)
			ch <- &TimeSeriesEntry{BytesToFloat(valBs), stamp}
		}
		iter.Release()
		close(ch)
	}()
	return ch, nil
}

// Close closes the db, flushing it eventually
func (store *LevelDBStorage) Close() error {
	return store.db.Close()
}
