package api

import (
	"column-prefetching-server/internal/service"
	"encoding/json"
	"net/http"
)

type ColumnPrefetchRequest struct {
	Bucket  string   `json:"bucket"`
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
		Columns: apiReq.Columns,
	}

	api.PrefetchingService.PrefetchColumns(r.Context(), prefetchRequest)

	w.WriteHeader(http.StatusAccepted)
}
