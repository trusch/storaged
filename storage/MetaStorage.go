package storage

import (
	"errors"
	"net/url"
	"time"
)

// MetaStorage wraps a specific storage
type MetaStorage struct {
	base Storage
}

// NewMetaStorage returns a new Storage object with the correct implementation for the given URI
func NewMetaStorage(uriStr string) (Storage, error) {
	uri, err := url.Parse(uriStr)
	if err != nil {
		return nil, err
	}
	var base Storage
	switch uri.Scheme {
	case "leveldb":
		base, err = NewLevelDBStorage(uri.Host + uri.Path)
	case "bolt":
		base, err = NewBoltStorage(uri.Host + uri.Path)
	case "mongodb":
		base, err = NewMongoStorage(uriStr)
	default:
		err = errors.New("unknown uri scheme, try bolt:// or leveldb://")
	}
	if err != nil {
		return nil, err
	}
	return &MetaStorage{base}, nil
}

func (store *MetaStorage) Put(key string, value []byte) error {
	return store.base.Put(key, value)
}

func (store *MetaStorage) Get(key string) ([]byte, error) {
	return store.base.Get(key)
}

func (store *MetaStorage) Delete(key string) error {
	return store.base.Delete(key)
}

func (store *MetaStorage) AddValue(key string, value float64) error {
	return store.base.AddValue(key, value)
}

func (store *MetaStorage) GetRange(key string, from time.Time, to time.Time) (chan *TimeSeriesEntry, error) {
	return store.base.GetRange(key, from, to)
}

func (store *MetaStorage) Close() error {
	return store.base.Close()
}
