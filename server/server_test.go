package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/trusch/storaged/storage"
)

type ServerSuite struct {
	suite.Suite
	srv *Server
}

func (suite *ServerSuite) SetupSuite() {
	store, err := storage.NewLevelDBStorage("./test-store")
	suite.NoError(err)
	suite.NotEmpty(store)
	suite.srv = New(":8080", store)
	go suite.srv.ListenAndServe()
	time.Sleep(200 * time.Millisecond)
}

func (suite *ServerSuite) TearDownTest() {
	err := suite.srv.store.Close()
	suite.NoError(err)
	err = os.RemoveAll("./test-store")
	suite.NoError(err)
	store, err := storage.NewLevelDBStorage("./test-store")
	suite.NoError(err)
	suite.NotEmpty(store)
	suite.srv.store = store
}

func (suite *ServerSuite) TearDownSuite() {
	err := suite.srv.Stop()
	suite.NoError(err)
	err = os.RemoveAll("./test-store")
	suite.NoError(err)
}

func (suite *ServerSuite) TestPut() {
	res, err := suite.request("PUT", "/kv/foo", "hello world")
	suite.NoError(err)
	suite.Empty(res)
}

func (suite *ServerSuite) TestGet() {
	res, err := suite.request("PUT", "/kv/foo", "hello world")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("GET", "/kv/foo", "")
	suite.NoError(err)
	suite.Equal("hello world", res)
}

func (suite *ServerSuite) TestDelete() {
	res, err := suite.request("PUT", "/kv/foo", "hello world")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("DELETE", "/kv/foo", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("GET", "/kv/foo", "")
	suite.Equal("404", err.Error())
	suite.Empty(res)
}

func (suite *ServerSuite) TestAddValue() {
	res, err := suite.request("POST", "/ts/test", "value=123.123")
	suite.NoError(err)
	suite.Empty(res)
}

func (suite *ServerSuite) TestBadAddValue() {
	_, err := suite.request("POST", "/ts/test", "")
	suite.Equal("400", err.Error())
	_, err = suite.request("POST", "/ts/test", "value=abc")
	suite.Equal("400", err.Error())
}

func (suite *ServerSuite) TestGetRange() {
	res, err := suite.request("POST", "/ts/foo?value=0", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("POST", "/ts/foo?value=1", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("POST", "/ts/foo?value=2", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("POST", "/ts/foo?value=3", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("GET", "/ts/foo", "")
	suite.NoError(err)
	slice := make([]map[string]interface{}, 0)
	decoder := json.NewDecoder(strings.NewReader(res))
	err = decoder.Decode(&slice)
	suite.NoError(err)
	suite.Equal(4, len(slice))
	suite.Equal(0., slice[0]["value"])
	suite.Equal(1., slice[1]["value"])
	suite.Equal(2., slice[2]["value"])
	suite.Equal(3., slice[3]["value"])
}

func (suite *ServerSuite) TestDeleteRange() {
	res, err := suite.request("POST", "/ts/foo?value=0", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("POST", "/ts/foo?value=1", "")
	suite.NoError(err)
	suite.Empty(res)

	t := time.Now()

	res, err = suite.request("POST", "/ts/foo?value=2", "")
	suite.NoError(err)
	suite.Empty(res)
	res, err = suite.request("POST", "/ts/foo?value=3", "")
	suite.NoError(err)
	suite.Empty(res)

	_, err = suite.request("DELETE", fmt.Sprintf("/ts/foo?to=%v", t.UnixNano()), "")
	suite.NoError(err)

	res, err = suite.request("GET", "/ts/foo", "")
	suite.NoError(err)
	slice := make([]map[string]interface{}, 0)
	decoder := json.NewDecoder(strings.NewReader(res))
	err = decoder.Decode(&slice)
	suite.NoError(err)
	suite.Equal(2, len(slice))
	suite.Equal(2., slice[0]["value"])
	suite.Equal(3., slice[1]["value"])
}

func (suite *ServerSuite) TestGetRangeWithN() {
	from := time.Now()
	var (
		count = 1000
		n     = 25
	)
	for i := 0; i < count; i++ {
		res, err := suite.request("POST", "/ts/foo", "value=1")
		suite.NoError(err)
		suite.Empty(res)
	}
	res, err := suite.request("GET", "/ts/foo?"+fmt.Sprintf("n=%v&from=%v", n, from.UnixNano()), "")
	suite.NoError(err)
	slice := make([]map[string]interface{}, 0)
	decoder := json.NewDecoder(strings.NewReader(res))
	err = decoder.Decode(&slice)
	suite.NoError(err)
	suite.True(math.Abs(1.-(float64(len(slice))/float64(n))) < 0.15, fmt.Sprintf("%v", math.Abs(1.-(float64(len(slice))/float64(n)))))
}

func (suite *ServerSuite) request(method, path string, data string) (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, fmt.Sprintf("http://localhost:8080/v1%v", path), strings.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("%v", resp.StatusCode)
	}
	return string(body), nil
}

func TestServer(t *testing.T) {
	suite.Run(t, new(ServerSuite))
}
