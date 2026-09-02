package worker

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/queue"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
	"github.com/nats-io/nats.go/jetstream"
)

type Pool struct {
	store  job.Store
	files  storage.Storage
	ffmpeg *ffmpeg.Runner
	js jetstream.JetStream
}

func NewPool(size int, store job.Store, files storage.Storage, runner *ffmpeg.Runner, js jetstream.JetStream) *Pool {
	p := &Pool{
		store:  store,
		files:  files,
		ffmpeg: runner,
		js: js,
	}

	ctx := context.Background();
	if err := queue.EnsureStream(ctx, js); err != nil {
		log.Fatalf("failed to ensure stream: %v", err)
	}

	cons, err := queue.CreateConsumer(ctx, js);
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	for i := 0; i < size; i++ {
		go p.startWorker(i, cons)
	}

	return p
}

func (p *Pool) Enqueue(j *job.Job) error {
	j.Status = job.StatusQueued;
	if err := p.store.Save(j); err != nil {
		return err;
	}

	data, err := json.Marshal(j);
	if err != nil {
		return err
	}

	ctx := context.Background()
	return queue.Publish(ctx, p.js, data);
}

func (p *Pool) startWorker(id int, cons jetstream.Consumer) {
	for {
		msgs, err := cons.Fetch(1);
		if err != nil {
			log.Printf("worker %d: fetch error: %v", id, err)
			continue
		}

		for msg := range msgs.Messages() {
			var j job.Job;
			if err := json.Unmarshal(msg.Data(), &j); err != nil {
				log.Printf("worker %d: failed to unmarshal job: %v", id, err)
				msg.Ack()
				continue
			}

			p.process(id, &j);
			msg.Ack();
		}
	}
}

func (p *Pool) process(workerID int, j *job.Job) {
	// mark process as processing
	j.Status = job.StatusProcessing
	if err := p.store.Save(j); err != nil {
		log.Printf("worker %d: failed to mark job %s as processing: %v", workerID, j.ID, err)
		return
	}

	// open the file
	inputFile, err := p.files.Open(j.InputKey);
	if err != nil {
		log.Printf("worker %d: failed to open input for job %s: %v", workerID, j.ID, err)
		j.Status = job.StatusFailed;
		j.Error = err.Error();
		p.store.Save(j);
		return;
	}

	// close the file as well
	defer inputFile.Close();

	// create temporary file paths
	// inputFile is reader -> but ffmpeg needs actual path
	// ffmpeg -i /tmp/abc123-input.mp4 ...  -> needs path
	tempInputPath := filepath.Join(os.TempDir(), j.ID+"-input.mp4");
	tempOutputPath := filepath.Join(os.TempDir(), j.ID+"-output.mp4");

	tempFile, err := os.Create(tempInputPath);
	if err != nil {
		log.Printf("worker %d: failed to create temp file for job %s: %v", workerID, j.ID, err)
		j.Status = job.StatusFailed
		j.Error = err.Error()
		p.store.Save(j)
		return
	}

	// copy the file and close the file and delete it
	if _, err := io.Copy(tempFile, inputFile); err != nil {
		tempFile.Close();
		log.Printf("worker %d: failed to copy input for job %s: %v", workerID, j.ID, err)
		j.Status = job.StatusFailed
		j.Error = err.Error()
		p.store.Save(j)
		return
	}
	tempFile.Close();
	defer os.Remove(tempInputPath);

	// transcode now
	ctx := context.Background();
	err = p.ffmpeg.Transcode(ctx, tempInputPath, tempOutputPath, "medium");
	if err != nil {
		log.Printf("worker %d: job %s failed: %v", workerID, j.ID, err)
		j.Status = job.StatusFailed
		j.Error = err.Error()
		p.store.Save(j)
		return
	}
	defer os.Remove(tempOutputPath);

	outputFile, err := os.Open(tempOutputPath);
	if err != nil {
		log.Printf("worker %d: failed to open transcoded output for job %s: %v", workerID, j.ID, err)
		j.Status = job.StatusFailed
		j.Error = err.Error()
		p.store.Save(j)
		return
	}
	defer outputFile.Close();

	outputKey, err := p.files.Save(j.ID+"-output.mp4", outputFile);
	if err != nil {
		log.Printf("worker %d: failed to save output for job %s: %v", workerID, j.ID, err)
		j.Status = job.StatusFailed
		j.Error = err.Error()
		p.store.Save(j)
		return
	}

	j.Status = job.StatusDone;
	j.OutputKeys = []string{outputKey};
	p.store.Save(j)
	log.Printf("worker %d: job %s done, output=%s", workerID, j.ID, outputKey)
}
