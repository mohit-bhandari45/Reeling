package worker

import (
	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
)

type Pool struct {
	jobs chan *job.Job
	store job.Store
	files storage.Storage
	ffmpeg *ffmpeg.Runner
}

func NewPool(size int, store job.Store, files storage.Storage, runner *ffmpeg.Runner) *Pool {
	p := &Pool{
		jobs: make(chan *job.Job, 100),
		store: store,
		files: files,
		ffmpeg: runner,
	}

	for i:=0;i<size;i++ {
		go p.startWorker(i);
	}

	return p;
}

func (p *Pool) startWorker(id int) {
	for j := range p.jobs {
		p.process(id, j);
	}
}