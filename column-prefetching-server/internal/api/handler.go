package api

import (
	"column-prefetching-server/internal/service"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ColumnPrefetchRequest represents the format of the incoming HTTP request from ALL.
type ColumnPrefetchRequest struct {
	Bucket  string   `json:"bucket"`
	Prefix  string   `json:"prefix"`
	Columns []string `json:"columns"`
}

func (api *API) HandlePrefetchColumns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var apiReq ColumnPrefetchRequest

	err := json.NewDecoder(r.Body).Decode(&apiReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	prefetchRequest := service.PrefetchRequest{
		Bucket:  apiReq.Bucket,
		Prefix:  apiReq.Prefix,
		Columns: apiReq.Columns,
	}

	startTime := time.Now()
	api.PrefetchingService.PrefetchColumns(r.Context(), prefetchRequest)
	elapsedTime := time.Since(startTime)

	fmt.Printf("Prefetching took: %f seconds\n", elapsedTime.Seconds())

	w.WriteHeader(http.StatusAccepted)
}
