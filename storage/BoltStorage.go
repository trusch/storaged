package storage

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

//BoltStorage is an implementation for KeyValueStorage and TimeSeriesStorage
type BoltStorage struct {
	db *bolt.DB
}

// NewBoltStorage creates a new storage instance
func NewBoltStorage(path string) (Storage, error) {
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
		b, key, err := store.getOrCreateBucketForKey(tx, "kv/"+key)
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
		b, k, err := store.getBucketForKey(tx, "kv/"+key)
		if err != nil {
			return err
		}
		value = b.Get([]byte(k))
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
		b, k, err := store.getBucketForKey(tx, "kv/"+key)
		if err != nil {
			return err
		}
		if err := b.Delete([]byte(k)); err != nil {
			return err
		}
		return nil
	})
}

// AddValue saves a value to the given timeseries
func (store *BoltStorage) AddValue(key string, value float64) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b, _, err := store.getOrCreateBucketForKey(tx, "ts/"+key+"/")
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
	go store.db.View(func(tx *bolt.Tx) error {
		b, _, err := store.getBucketForKey(tx, "ts/"+key+"/")
		if err != nil {
			close(ch)
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

func (store *BoltStorage) getBucketForKey(tx *bolt.Tx, key string) (*bolt.Bucket, string, error) {
	parts := strings.Split(key, "/")
	bucket := tx.Bucket([]byte(parts[0]))
	if bucket == nil {
		return nil, "", errors.New("bucket not found")
	}
	parts = parts[1:]
	for len(parts) > 1 {
		bucket = bucket.Bucket([]byte(parts[0]))
		if bucket == nil {
			return nil, "", errors.New("bucket not found")
		}
		parts = parts[1:]
	}
	return bucket, parts[0], nil
}

func (store *BoltStorage) getOrCreateBucketForKey(tx *bolt.Tx, key string) (*bolt.Bucket, string, error) {
	parts := strings.Split(key, "/")
	bucket, err := tx.CreateBucketIfNotExists([]byte(parts[0]))
	if err != nil {
		return nil, "", err
	}
	parts = parts[1:]
	for len(parts) > 1 {
		bucket, err = bucket.CreateBucketIfNotExists([]byte(parts[0]))
		if err != nil {
			return nil, "", err
		}
		parts = parts[1:]
	}
	return bucket, parts[0], nil
}

// Close closes the db, flushing it eventually
func (store *BoltStorage) Close() error {
	return store.db.Close()
}
