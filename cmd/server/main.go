package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/mohit-bhandari45/Reeling/internal/api"
	"github.com/mohit-bhandari45/Reeling/internal/ffmpeg"
	"github.com/mohit-bhandari45/Reeling/internal/job"
	"github.com/mohit-bhandari45/Reeling/internal/queue"
	"github.com/mohit-bhandari45/Reeling/internal/storage"
	"github.com/mohit-bhandari45/Reeling/internal/worker"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from real environment")
	}
	s3Client, err := storage.NewS3Client("http://localhost:9000", os.Getenv("MINIO_ROOT_USER"), os.Getenv("MINIO_ROOT_PASSWORD"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := storage.EnsureBucket(ctx, s3Client, "reeling-videos"); err != nil {
		log.Fatal(err)
	}

	fileStorage := storage.NewS3Storage(s3Client, "reeling-videos")

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
	jobStore := job.NewPostgresStore(db)

	runner := ffmpeg.NewRunner()

	nc, js, err := queue.NewConn("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	pool := worker.NewPool(100, jobStore, fileStorage, runner, js)

	server := api.NewServer(fileStorage, jobStore, pool)

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
