package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/mohit-bhandari45/Reeling/internal/storage"
)

var fileStorage storage.Storage

func main() {
	var err error
	fileStorage, err := storage.NewLocalDisk("./data/uploads")
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/videos", HandleUpload)

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
	}
	defer file.Close()

	key, err := fileStorage.Save(header.Filename, file)
	if err != nil {
		log.Printf("failed to save upload: %v", err)
		http.Error(w, "failed to save file", http.StatusInternalServerError)
		return
	}
	log.Printf("saved upload: %s -> key=%s", header.Filename, key)

	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": "fake-job-id-123",
		"input_key" : key,
	})
}
