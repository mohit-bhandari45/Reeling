package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
	"github.com/mohit-bhandari45/Reeling/internal/worker"
)

type Server struct {
	files storage.Storage
	jobs job.Store
	pool *worker.Pool
}

func NewServer(files storage.Storage, jobs job.Store, pool *worker.Pool) *Server {
	return &Server{
		files: files,
		jobs: jobs,
		pool: pool,
	}
}

func (s *Server) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		/* could be done:-
		w.WriteHeader(http.StatusMethodNotAllowed);
		w.Write([]byte("method not allowed"))
		*/

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	file, header, err := r.FormFile("video")
	if err != nil {
		http.Error(w, "missing 'video' field in form data", http.StatusBadRequest)
		return;
	}
	defer file.Close()

	key, err := s.files.Save(header.Filename, file)
	if err != nil {
		slog.Error("failed to save upload", "error", err)
		http.Error(w, "failed to save file", http.StatusInternalServerError)
		return
	}

	webhookURL := r.FormValue("webhook_url");

	var renditions []string
	if r.FormValue("renditions") != "" {
		renditions = strings.Split(r.FormValue("renditions"), ",");
	}
	
	j := &job.Job{
		ID: uuid.NewString(),
		InputKey: key,
		Status: job.StatusQueued,
		WebhookURL: webhookURL,
		Renditions: renditions,
	}
	if err := s.pool.Enqueue(j); err != nil {
		slog.Error("failed to enqueue job", "job_id", j.ID, "error", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return;
	}

	slog.Info("job created",
		"job_id", j.ID,
		"input_key", key,
	)

	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(j);
}

func (s *Server) HandleGetJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed);
		return;
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/");
	if id == "" {
		http.Error(w, "missing job id", http.StatusBadRequest)
		return
	}

	j, err := s.jobs.Get(id);
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j);
}