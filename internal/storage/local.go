package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"uuid"

	"github.com/google/uuid"
)

type LocalDisk struct {
	BaseDir string
}

func NewLocalDisk(baseDir string) (*LocalDisk, error) {
	if err := os.MkdirAll(baseDir, 0o755); err!=nil {
		return nil, err;
	}
	return &LocalDisk{BaseDir: baseDir}, nil
}

func (d *LocalDisk) Save(name string, r io.Reader) (string, error) {
	key := fmt.Sprintf("%s-%s", uuid.NewString(), filepath.Base(name));
	path := filepath.Join(d.BaseDir, key);

	f, err := os.Create(path);
	if err != nil {
		return "", err
	}
	defer f.Close();

	if _, err := io.Copy(f, r); err != nil {
		return "", nil
	}
	return key, nil;
}

func (d *LocalDisk) Get(key string) (io.Reader, error) {
	return os.Open(filepath.Join(d.BaseDir, key));
}