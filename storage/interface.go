package storage

import "time"

// KeyValueStorage is the interface for key-value-storage backends
type KeyValueStorage interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

// A TimeSeriesEntry is a single entry of a timeseries
type TimeSeriesEntry struct {
	Value     float64
	Timestamp time.Time
}

// TimeSeriesStorage is the interface for timeseries handling
type TimeSeriesStorage interface {
	AddValue(key string, value float64) error
	GetRange(key string, from time.Time, to time.Time) (chan *TimeSeriesEntry, error)
}

// Storage is a combined interface of KeyValueStorage and TimeSeriesStorage
type Storage interface {
	KeyValueStorage
	TimeSeriesStorage
	Close() error
}
