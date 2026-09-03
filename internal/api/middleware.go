package api

import "net/http";

func RequireAPIKey(apiKey string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key");
		if key == "" || key != apiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return;
		}

		next(w, r);
	}
}