package storage

import (
	"strings"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MongoStorage is an implementation for KeyValueStorage and TimeSeriesStorage
type MongoStorage struct {
	db      *mgo.Database
	session *mgo.Session
}

type kvEntry struct {
	Key   string `bson:"k"`
	Value []byte `bson:"v"`
}

type tsEntry struct {
	Key   int64   `bson:"k"`
	Value float64 `bson:"v"`
}

// NewMongoStorage creates a new storage with mongodb as its backend
func NewMongoStorage(url string) (Storage, error) {
	info, err := mgo.ParseURL(url)
	if err != nil {
		return nil, err
	}
	s, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, err
	}
	return &MongoStorage{s.DB(info.Database), s}, nil
}

// Put stores data in the db
// seperate collections can be specified by using slashes in the key
// -> Put("foo/bar", "baz") will create a doc with key bar in collection foo (containing baz)
func (store *MongoStorage) Put(key string, value []byte) error {
	c, keyName, err := store.getCollectionAndKey("kv/" + key)
	if err != nil {
		return err
	}
	return c.Insert(bson.M{"k": keyName, "v": value})
}

// Get retrieves a doc from the db
func (store *MongoStorage) Get(key string) ([]byte, error) {
	c, keyName, err := store.getCollectionAndKey("kv/" + key)
	if err != nil {
		return nil, err
	}
	res := &kvEntry{}
	err = c.Find(bson.M{"k": keyName}).One(res)
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

// Delete drops an entry from db
func (store *MongoStorage) Delete(key string) error {
	c, keyName, err := store.getCollectionAndKey("kv/" + key)
	if err != nil {
		return err
	}
	return c.Remove(bson.M{"k": keyName})
}

// AddValue adds a value to a timeseries
func (store *MongoStorage) AddValue(key string, value float64) error {
	c, _, err := store.getCollectionAndKey("ts/" + key + "/")
	if err != nil {
		return err
	}
	return c.Insert(bson.M{"k": time.Now().UnixNano(), "v": value})
}

// GetRange returns a aspecific range in a timeseries
func (store *MongoStorage) GetRange(key string, from time.Time, to time.Time) (chan *TimeSeriesEntry, error) {
	c, _, err := store.getCollectionAndKey("ts/" + key + "/")
	if err != nil {
		return nil, err
	}
	ch := make(chan *TimeSeriesEntry, 64)
	iter := c.Find(bson.M{"k": bson.M{"$gte": from.UnixNano(), "$lte": to.UnixNano()}}).Iter()
	go func() {
		entry := &tsEntry{}
		for iter.Next(entry) {
			ch <- &TimeSeriesEntry{entry.Value, time.Unix(0, entry.Key)}
		}
		close(ch)
	}()
	return ch, nil
}

// DeleteRange returns a aspecific range in a timeseries
func (store *MongoStorage) DeleteRange(key string, from time.Time, to time.Time) error {
	c, _, err := store.getCollectionAndKey("ts/" + key + "/")
	if err != nil {
		return err
	}

	_, err = c.RemoveAll(bson.M{"k": bson.M{"$gte": from.UnixNano(), "$lte": to.UnixNano()}})
	return err
}

// Close closes the db
func (store *MongoStorage) Close() error {
	store.session.Close()
	return nil
}

func (store *MongoStorage) getCollectionAndKey(str string) (*mgo.Collection, string, error) {
	idx := strings.LastIndex(str, "/")
	c := store.db.C(str[:idx])
	index := mgo.Index{
		Key:        []string{"k"},
		Unique:     true,
		DropDups:   true,
		Background: true,
	}
	err := c.EnsureIndex(index)
	return c, str[idx+1:], err
}
