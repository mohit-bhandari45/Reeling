package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

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
	start := time.Now()

	// check if this is already processed
	existing, err := p.store.Get(j.ID);
	if err == nil && (existing.Status == job.StatusDone || existing.Status == job.StatusFailed) {
		slog.Info("job already finished, skipping reprocessing",
			"worker_id", workerID,
			"job_id", j.ID,
			"status", existing.Status,
		)
		return;
	}

	// mark process as processing
	j.Status = job.StatusProcessing
	if err := p.store.Save(j); err != nil {
		p.handleFailure(workerID, j, err)
		return
	}

	// open the file
	inputFile, err := p.files.Open(j.InputKey);
	if err != nil {
		p.handleFailure(workerID, j, err);
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
		p.handleFailure(workerID, j, err);
		return
	}

	// copy the file and close the file and delete it
	if _, err := io.Copy(tempFile, inputFile); err != nil {
		tempFile.Close();
		p.handleFailure(workerID, j, err);
		return
	}
	tempFile.Close();
	defer os.Remove(tempInputPath);

	// transcode now
	ctx := context.Background();
	err = p.ffmpeg.Transcode(ctx, tempInputPath, tempOutputPath, "medium");
	if err != nil {
		p.handleFailure(workerID, j, err);
		return
	}
	defer os.Remove(tempOutputPath);

	// open the file
	outputFile, err := os.Open(tempOutputPath);
	if err != nil {
		p.handleFailure(workerID, j, err);
		return
	}
	defer outputFile.Close();

	// save the file to the disk / minio
	outputKey, err := p.files.Save(j.ID+"-output.mp4", outputFile);
	if err != nil {
		p.handleFailure(workerID, j, err);
		return
	}

	// mark as done
	j.Status = job.StatusDone;
	j.OutputKeys = []string{outputKey};
	p.store.Save(j)
	slog.Info("job completed",
		"worker_id", workerID,
		"job_id", j.ID,
		"output_key", outputKey,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	p.sendWebhook(workerID, j);
}

const MaxAttempts = 3;

func (p *Pool) handleFailure(workerID int, j *job.Job, cause error) {
	j.Attempts++;
	j.Error = cause.Error();

	if j.Attempts >= MaxAttempts {
		j.Status = job.StatusFailed;
		p.store.Save(j);
		slog.Error("job permanently failed",
			"worker_id", workerID,
			"job_id", j.ID,
			"attempts", j.Attempts,
			"error", cause,
		)
		p.sendWebhook(workerID, j);
		return
	}

	j.Status = job.StatusQueued;
	p.store.Save(j);

	data, err := json.Marshal(j);
	if err != nil {
		slog.Error("failed to marshal job for retry",
			"worker_id", workerID,
			"job_id", j.ID,
			"error", err,
		)
		return
	}

	ctx := context.Background();
	if err := queue.Publish(ctx, p.js, data); err != nil {
		slog.Error("failed to republish job for retry",
			"worker_id", workerID,
			"job_id", j.ID,
			"error", err,
		)
		return
	}

	slog.Warn("job failed, requeued for retry",
		"worker_id", workerID,
		"job_id", j.ID,
		"attempt", j.Attempts,
		"max_attempts", MaxAttempts,
		"error", cause,
	)
}

func (p *Pool) sendWebhook(workerID int, j *job.Job) {
	if j.WebhookURL == "" {
		return;
	}

	data, err := json.Marshal(j);
	if err != nil {
		slog.Error("failed to marshal job for webhook", "worker_id", workerID, "job_id", j.ID, "error", err)
		return
	}

	resp, err := http.Post(j.WebhookURL, "application/json", bytes.NewReader(data));
	if err != nil {
		slog.Error("failed to send webhook", "worker_id", workerID, "job_id", j.ID, "webhook_url", j.WebhookURL, "error", err)
		return
	}
	defer resp.Body.Close()

	slog.Info("webhook sent",
		"worker_id", workerID,
		"job_id", j.ID,
		"webhook_url", j.WebhookURL,
		"status_code", resp.StatusCode,
	)
}