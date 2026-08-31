package main

import (
	"encoding/json"
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/videos", HandleUpload);

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

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed);
		return;
	}

	file, header, err := r.FormFile("video");
	if err != nil {
		http.Error(w, "missing 'video' field in form data", http.StatusBadRequest);
	}

	defer file.Close();
	log.Printf("received upload: %s (%d bytes)", header.Filename, header.Size)

	w.Header().Set("Content-type", "application/json");
	w.WriteHeader(http.StatusAccepted);
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": "fake-job-id-123",
	});
}