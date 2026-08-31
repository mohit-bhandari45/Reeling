package worker

import (
	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
)

type Pool struct {
	jobs chan *job.Job
	store job.Store
	storage storage.Storage
	ffmpeg *ffmpeg.Runner
}

