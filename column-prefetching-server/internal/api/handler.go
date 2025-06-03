package api

import (
	"net/http"
	"strings"
)

type ColumnPrefetchRequest struct {
	Bucket  string   `json:"bucket"`
	Columns []string `json:"columns"`
}

func PrefetchColumns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
}

func PrefetchHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/prefetch/"):
		PrefetchColumns(w, r)
	}
}
