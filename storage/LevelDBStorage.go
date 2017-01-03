package storage

import (
	"bytes"
	"encoding/gob"
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
	gob.Register(Object{})
	gob.Register([]Object{})
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
func (store *LevelDBStorage) Put(key string, value Object) error {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(value)
	if err != nil {
		return err
	}
	return store.db.Put([]byte(key), buf.Bytes(), nil)
}

// Get retrieves a value from db
func (store *LevelDBStorage) Get(key string) (Object, error) {
	bs, err := store.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(bytes.NewReader(bs))
	result := Object{}
	return result, decoder.Decode(&result)
}

// Delete drops an entry from db
func (store *LevelDBStorage) Delete(key string) error {
	return store.db.Delete([]byte(key), nil)
}

// AddValue saves a value to the given timeseries
func (store *LevelDBStorage) AddValue(key string, value float64) error {
	keyBs := []byte(fmt.Sprintf("%v%v", key, time.Now().UnixNano()))
	valBs := FloatToBytes(value)
	return store.db.Put(keyBs, valBs, nil)
}

// GetRange returns a channel which will give all values in a timerange
func (store *LevelDBStorage) GetRange(key string, from time.Time, to time.Time) (chan *TimeSeriesEntry, error) {
	ch := make(chan *TimeSeriesEntry, 64)
	startKey := []byte(fmt.Sprintf("%v%v", key, from.UnixNano()))
	endKey := []byte(fmt.Sprintf("%v%v", key, to.UnixNano()))
	iter := store.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
	if err := iter.Error(); err != nil {
		return nil, err
	}
	go func() {
		for iter.Next() {
			keyBs := iter.Key()
			valBs := iter.Value()
			nanos, err := strconv.ParseInt(string(keyBs[len(key):]), 10, 64)
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
