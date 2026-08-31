package storage

import "io"

type Storage interface {
	Save(name string, r io.Reader) (key string, err error)
	Open(key string) (io.ReadCloser, error)
}