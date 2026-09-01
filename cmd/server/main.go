package main

import (
	"log"
	"net/http"

	"github.com/mohit-bhandari45/Reeling/internal/api"
	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
	"github.com/mohit-bhandari45/Reeling/internal/worker"
)

func main() {
	fileStorage, err := storage.NewLocalDisk("./data/uploads")
	if err != nil {
		log.Fatal(err)
	}

	db, err := job.NewPostgresConn("postgres://reeling:reeling@localhost:5432/reeling")
	if err != nil {
		log.Fatal(err)
	}
	jobStore := job.NewPostgresStore(db);

	runner := ffmpeg.NewRunner();
	pool := worker.NewPool(100, jobStore, fileStorage, runner);
	
	server := api.NewServer(fileStorage, jobStore, pool);

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/videos", server.HandleUpload)
	mux.HandleFunc("/jobs/", server.HandleGetJob)

	log.Println("Server started on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}