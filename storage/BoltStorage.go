package storage

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

//BoltStorage is an implementation for KeyValueStorage and TimeSeriesStorage
type BoltStorage struct {
	db *bolt.DB
}

// NewBoltStorage creates a new storage instance
func NewBoltStorage(path string) (*BoltStorage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	store := &BoltStorage{db}
	return store, nil
}

// Put saves a value to the db
func (store *BoltStorage) Put(key string, value []byte) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("kv"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte(key), value); err != nil {
			return err
		}
		return nil
	})
}

// Get retrieves a value from db
func (store *BoltStorage) Get(key string) ([]byte, error) {
	var value []byte
	err := store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("kv"))
		if b == nil {
			return errors.New("no such bucket")
		}
		value = b.Get([]byte(key))
		if value == nil {
			return errors.New("no such value")
		}
		return nil
	})
	return value, err
}

// Delete drops an entry from db
func (store *BoltStorage) Delete(key string) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("kv"))
		if b == nil {
			return errors.New("no such bucket")
		}
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
		return nil
	})
}

// AddValue saves a value to the given timeseries
func (store *BoltStorage) AddValue(key string, value float64) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("ts/" + key))
		if err != nil {
			return err
		}
		key = fmt.Sprintf("%v", time.Now().UnixNano())
		valBs := FloatToBytes(value)
		if err := b.Put([]byte(key), valBs); err != nil {
			return err
		}
		return nil
	})
}

// GetRange returns a channel which will give all values in a timerange
func (store *BoltStorage) GetRange(key string, from time.Time, to time.Time) (chan *TimeSeriesEntry, error) {
	ch := make(chan *TimeSeriesEntry, 64)
	var err error
	go store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("ts/" + key))
		if b == nil {
			err = errors.New("no such timeseries")
			return err
		}
		c := b.Cursor()
		startKey := []byte(fmt.Sprintf("%v", from.UnixNano()))
		endKey := []byte(fmt.Sprintf("%v", to.UnixNano()))
		for k, v := c.Seek(startKey); k != nil && bytes.Compare(k, endKey) <= 0; k, v = c.Next() {
			nanos, err := strconv.ParseInt(string(k), 10, 64)
			if err != nil {
				continue
			}
			stamp := time.Unix(0, nanos)
			ch <- &TimeSeriesEntry{BytesToFloat(v), stamp}
		}
		close(ch)
		return nil
	})
	return ch, nil
}

// Close closes the db, flushing it eventually
func (store *BoltStorage) Close() error {
	return store.db.Close()
}
