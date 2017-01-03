package server

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/trusch/storaged/storage"
)

// Server represents the storaged webserver
type Server struct {
	store  storage.Storage
	ln     net.Listener
	server *http.Server
}

// New creates a new webserver
func New(addr string, store storage.Storage) *Server {
	srv := &http.Server{
		Addr:           addr,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	server := &Server{store, nil, srv}
	server.constructRouter()
	return server
}

// ListenAndServe starts the webserver
func (srv *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", srv.server.Addr)
	if err != nil {
		return err
	}
	srv.ln = ln
	return srv.server.Serve(ln)
}

// Stop stops the webserver
func (srv *Server) Stop() error {
	err := srv.ln.Close()
	if err != nil {
		return err
	}
	return srv.store.Close()
}

func (srv *Server) constructRouter() {
	router := mux.NewRouter()
	router.PathPrefix("/v1/kv/").Methods("PUT").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.handlePut(w, r)
	})
	router.PathPrefix("/v1/kv/").Methods("GET").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.handleGet(w, r)
	})
	router.PathPrefix("/v1/kv/").Methods("DELETE").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.handleDelete(w, r)
	})
	router.PathPrefix("/v1/ts/").Methods("POST").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.handleAddValue(w, r)
	})
	router.PathPrefix("/v1/ts/").Methods("GET").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.handleGetRange(w, r)
	})
	srv.server.Handler = router
}

func (srv *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Print("failed put: ", r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	key := r.URL.Path
	err = srv.store.Put(key, bs)
	if err != nil {
		log.Print("failed put: ", r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	bs, err := srv.store.Get(key)
	if err != nil {
		log.Print("failed get: ", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write(bs)
}

func (srv *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	err := srv.store.Delete(key)
	if err != nil {
		log.Print("failed delete: ", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (srv *Server) handleAddValue(w http.ResponseWriter, r *http.Request) {
	floatStr := r.FormValue("value")
	if floatStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("need 'value'"))
		return
	}
	val, err := strconv.ParseFloat(floatStr, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("'value' needs to be a float"))
		return
	}
	key := r.URL.Path
	err = srv.store.AddValue(key, val)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (srv *Server) handleGetRange(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	n := r.FormValue("n")
	var desiredPoints int64
	if n != "" {
		dp, e := strconv.ParseInt(n, 10, 64)
		if e != nil {
			log.Print("malformed N option")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		desiredPoints = dp
	}
	f, _ := strconv.ParseInt(r.FormValue("from"), 10, 64)
	t, _ := strconv.ParseInt(r.FormValue("to"), 10, 64)
	if t == 0 {
		t = time.Now().UnixNano()
	}
	from := time.Unix(0, f)
	to := time.Unix(0, t)
	ch, err := srv.store.GetRange(key, from, to)
	if err != nil || ch == nil {
		log.Print("fail...")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if desiredPoints > 0 {
		ch = reduceStream(ch, from, to, desiredPoints)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("["))
	first := true
	for pair := range ch {
		if bs, err := json.Marshal(map[string]interface{}{
			"timestamp": pair.Timestamp.UnixNano(),
			"value":     pair.Value,
		}); err == nil {
			if first {
				first = false
			} else {
				w.Write([]byte{','})
			}
			w.Write(bs)
		} else {
			log.Print(err)
		}
	}
	w.Write([]byte("]"))
}

func reduceStream(input chan *storage.TimeSeriesEntry, from, to time.Time, desiredPoints int64) chan *storage.TimeSeriesEntry {
	output := make(chan *storage.TimeSeriesEntry, 64)
	timeSpan := to.Sub(from)
	interval := int64(timeSpan.Nanoseconds()) / desiredPoints
	lastPoint := time.Time{}
	go func() {
		for pair := range input {
			if int64(pair.Timestamp.Sub(lastPoint).Nanoseconds()) >= interval {
				lastPoint = pair.Timestamp
				output <- pair
			}
		}
		close(output)
	}()
	return output
}
