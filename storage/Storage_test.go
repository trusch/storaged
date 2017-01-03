package storage

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type StorageSuite struct {
	suite.Suite
	store Storage
}

func (suite *StorageSuite) toBytes(val interface{}) []byte {
	bs, err := json.Marshal(val)
	suite.NoError(err)
	return bs
}

func (suite *StorageSuite) TearDownTest() {
	err := suite.store.Close()
	suite.NoError(err)
	os.RemoveAll("./test-store")
	store, err := NewLevelDBStorage("./test-store")
	suite.NoError(err)
	suite.NotNil(store)
	suite.store = store
}

func (suite *StorageSuite) TearDownSuite() {
	os.RemoveAll("./test-store")
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

func TestStorage(t *testing.T) {
	store, err := NewLevelDBStorage("./test-store")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	s := new(StorageSuite)
	s.store = store
	suite.Run(t, s)
}
