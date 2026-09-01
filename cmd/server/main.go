package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/mohit-bhandari45/Reeling/internal/api"
	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
	"github.com/mohit-bhandari45/Reeling/internal/worker"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from real environment")
	}

	fileStorage, err := storage.NewLocalDisk("./data/uploads")
	if err != nil {
		log.Fatal(err)
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@localhost:5432/%s",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_DB"),
	)

	db, err := job.NewPostgresConn(connString)
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