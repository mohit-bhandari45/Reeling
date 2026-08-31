package storage

import (
	"fmt"
	"io"
	"os"
)

type LocalDisk struct {
	BaseDir string
}

func NewLocalDisk(baseDir string) (*LocalDisk, error) {
	if err := os.MkdirAll(baseDir); err!=nil {
		return nil, err;
	}
	return &LocalDisk{BaseDir: baseDir}
}

