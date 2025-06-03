package app

import (
	"column-prefetching-server/internal/api"
	"column-prefetching-server/internal/project-config"
	"column-prefetching-server/internal/service"
	"log"

	"context"
	"net/http"
)

func main() {
	// initialise project-config
	cfg, _ := project_config.LoadConfig("project-config.json")

	// Initialise S3 service
	s3Service, err := service.NewS3Service(cfg.S3)
	if err != nil {
		log.Fatalf("Failed to create S3 service: %v", err)
	}

	mux := api.SetupRoutes()

	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		return
	}
}
