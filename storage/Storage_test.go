package storage

import (
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type StorageSuite struct {
	suite.Suite
	store Storage
	typ   string
}

func (suite *StorageSuite) toBytes(val interface{}) []byte {
	bs, err := json.Marshal(val)
	suite.NoError(err)
	return bs
}

func (suite *StorageSuite) clearStore() {
	if suite.typ == "mongo" {
		cmd := exec.Command("mongo", "test-store", "--eval", "db.kv.drop(); db['ts/test/value'].drop(); db['ts/test'].drop();")
		cmd.Run()
	} else {
		os.RemoveAll("./test-store.db")
	}
}

func (suite *StorageSuite) TearDownTest() {
	err := suite.store.Close()
	suite.NoError(err)
	suite.clearStore()
	switch suite.typ {
	case "leveldb":
		{
			store, err := NewMetaStorage("leveldb://test-store.db")
			suite.NoError(err)
			suite.NotNil(store)
			suite.store = store
		}
	case "bolt":
		{
			store, err := NewMetaStorage("bolt://test-store.db")
			suite.NoError(err)
			suite.NotNil(store)
			suite.store = store
		}
	case "mongo":
		{
			store, err := NewMetaStorage("mongodb://localhost/test-store")
			suite.NoError(err)
			suite.NotNil(store)
			suite.store = store
		}
	}
}

func (suite *StorageSuite) TearDownSuite() {
	suite.clearStore()
}

func (suite *StorageSuite) TestPut() {
	obj := suite.toBytes(map[string]interface{}{
		"int":    123,
		"float":  321.123,
		"string": "string",
		"object": map[string]interface{}{"foo": "bar"},
	})
	err := suite.store.Put("test", obj)
	suite.NoError(err)
}

func (suite *StorageSuite) TestComplexPut() {
	obj := suite.toBytes(map[string]interface{}{
		"object": []map[string]interface{}{map[string]interface{}{"foo": "bar"}},
	})
	err := suite.store.Put("test", obj)
	suite.NoError(err)
	restored, err := suite.store.Get("test")
	suite.NoError(err)
	suite.Equal(obj, restored)
}

func (suite *StorageSuite) TestGet() {
	obj := suite.toBytes(map[string]interface{}{
		"int":    123,
		"float":  321.123,
		"string": "string",
		"object": map[string]interface{}{"foo": "bar"},
	})
	err := suite.store.Put("test", obj)
	suite.NoError(err)
	restored, err := suite.store.Get("test")
	suite.NoError(err)
	suite.Equal(obj, restored)
}

func (suite *StorageSuite) TestGetNonExisting() {
	_, err := suite.store.Get("test")
	suite.Error(err)
}

func (suite *StorageSuite) TestDelete() {
	obj := suite.toBytes(map[string]interface{}{
		"int":    123,
		"float":  321.123,
		"string": "string",
		"object": map[string]interface{}{"foo": "bar"},
	})
	err := suite.store.Put("test", obj)
	suite.NoError(err)
	err = suite.store.Delete("test")
	suite.NoError(err)
	restored, err := suite.store.Get("test")
	suite.Error(err)
	suite.Nil(restored)
}

func (suite *StorageSuite) TestAddValue() {
	err := suite.store.AddValue("test", 123.123)
	suite.NoError(err)
}

func (suite *StorageSuite) TestAddValueNestedBucket() {
	err := suite.store.AddValue("test/value", 123.123)
	suite.NoError(err)
}

func (suite *StorageSuite) TestGetRange() {
	for i := 0; i < 100; i++ {
		err := suite.store.AddValue("test", float64(i))
		suite.NoError(err)
	}
	ch, err := suite.store.GetRange("test", time.Time{}, time.Now())
	suite.NoError(err)
	for i := 0; i < 100; i++ {
		kv, ok := <-ch
		suite.True(ok)
		suite.Equal(float64(i), kv.Value)
	}
	_, ok := <-ch
	suite.False(ok)
}

func (suite *StorageSuite) TestBadMetaStorageURI() {
	store, err := NewMetaStorage("wrong://uri")
	suite.Error(err)
	suite.Empty(store)
	store, err = NewMetaStorage("://foo")
	suite.Error(err)
	suite.Empty(store)
}

func (suite *StorageSuite) TestBadOpenRightsStorageURI() {
	store, err := NewMetaStorage("leveldb:///root/forbidden")
	suite.Error(err)
	suite.Empty(store)
	store, err = NewMetaStorage("bolt:///root/forbidden")
	suite.Error(err)
	suite.Empty(store)
}

func TestLevelDBStorage(t *testing.T) {
	store, err := NewMetaStorage("leveldb://test-store.db")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	s := new(StorageSuite)
	s.store = store
	s.typ = "leveldb"
	suite.Run(t, s)
}

func TestBoltStorage(t *testing.T) {
	store, err := NewMetaStorage("bolt://test-store.db")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	s := new(StorageSuite)
	s.store = store
	s.typ = "bolt"
	suite.Run(t, s)
}

func TestMongoStorage(t *testing.T) {
	store, err := NewMetaStorage("mongodb://localhost/test-store")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	s := new(StorageSuite)
	s.store = store
	s.typ = "mongo"
	suite.Run(t, s)
}
