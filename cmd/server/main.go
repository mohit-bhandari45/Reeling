package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
	"github.com/mohit-bhandari45/Reeling/internal/worker"
)

var fileStorage storage.Storage
var jobStore job.Store
var pool *worker.Pool

func main() {
	ls, err := storage.NewLocalDisk("./data/uploads")
	if err != nil {
		log.Fatal(err)
	}
	fileStorage = ls
	jobStore = job.NewMemoryStore()

	runner := ffmpeg.NewRunner();
	pool = worker.NewPool(100, jobStore, fileStorage, runner);

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/videos", HandleUpload)
	mux.HandleFunc("/jobs/", HandleGetJob)

	log.Println("Server started on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

func HandleUpload(w http.ResponseWriter, r *http.Request) {
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

	key, err := fileStorage.Save(header.Filename, file)
	if err != nil {
		log.Printf("failed to save upload: %v", err)
		http.Error(w, "failed to save file", http.StatusInternalServerError)
		return
	}
	
	j := &job.Job{
		ID: uuid.NewString(),
		InputKey: key,
		Status: job.StatusQueued,
	}
	if err := pool.Enqueue(j); err != nil {
		log.Printf("failed to save job: %v", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return;
	}

	log.Printf("created job %s for upload %s", j.ID, key)

	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(j);
}

func HandleGetJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed);
		return;
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/");
	if id == "" {
		http.Error(w, "missing job id", http.StatusBadRequest)
		return
	}

	j, err := jobStore.Get(id);
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j);
}