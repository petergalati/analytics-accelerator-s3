package service

import (
	project_config "column-prefetching-server/internal/project-config"
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"strings"
	"sync"
)

type PrefetchingService struct {
	s3Service        *S3Service
	cacheService     *CacheService
	concurrencyLimit int
}

type PrefetchRequest struct {
	Bucket  string
	Columns []string
}

type RequestedColumn struct {
	ColumnName string
	Start      int64
	End        int64
}
type ParquetColumnData struct {
	Bucket string
	File   string
	Column string
	Data   []byte
}

func NewPrefetchingService(
	s3Service *S3Service,
	cacheService *CacheService,
	cfg project_config.PrefetchingConfig,
) *PrefetchingService {
	return &PrefetchingService{
		s3Service:        s3Service,
		cacheService:     cacheService,
		concurrencyLimit: cfg.ConcurrencyLimit,
	}
}

func (service *PrefetchingService) PrefetchColumns(ctx context.Context, req PrefetchRequest) error {
	files, err := service.s3Service.ListParquetFiles(ctx, req.Bucket)

	if err != nil {
		return fmt.Errorf("failed to list parquet files: %w", err)
	}

	log.Printf("Found %d parquet files to process for columns: %v", len(files), req.Columns)

	// convert slice of columns to set of columns
	columnSet := make(map[string]struct{})
	for _, column := range req.Columns {
		columnSet[column] = struct{}{}
	}

	// creating a buffered channel, of length concurrencyLimit to act as a semaphore. This limits the number of concurrent goroutines.
	sem := make(chan struct{}, service.concurrencyLimit)
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)

		go func(file types.Object) {
			defer wg.Done()

			// acquire a resource by placing a value in the channel
			sem <- struct{}{}

			// release the resource when done
			defer func() { <-sem }()

			err := service.prefetchFileColumns(ctx, req.Bucket, file, columnSet)
			if err != nil {
				log.Printf("failed to process parquet file %q: %v", file, err)
			}
		}(file)
	}

	return nil
}

func (service *PrefetchingService) prefetchFileColumns(ctx context.Context, bucket string, file types.Object, columnSet map[string]struct{}) error {
	footerData, _ := service.s3Service.GetParquetFileFooter(ctx, bucket, *file.Key, *file.Size)

	requestedColumns, _ := getRequestedColumns(footerData, columnSet)

	sem := make(chan struct{}, service.concurrencyLimit)
	var wg sync.WaitGroup

	for _, requestedColumn := range requestedColumns {
		wg.Add(1)

		go func(requestedColumn RequestedColumn) {
			defer wg.Done()

			sem <- struct{}{}

			defer func() { <-sem }()

			columnData, _ := service.s3Service.GetColumnData(ctx, bucket, *file.Key, requestedColumn)

			service.cacheService.

		}(requestedColumn)
	}

}

func getRequestedColumns(footerData *metadata.FileMetaData, columnSet map[string]struct{}) ([]RequestedColumn, error) {
	// a list of requested columns to be prefetched
	var requestedColumns []RequestedColumn

	for _, rowGroup := range footerData.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			columnMetaData := columnChunk.MetaData

			columnName := strings.Join(columnMetaData.PathInSchema, ".")

			// we only want to process columns which are requested by AAL
			if _, found := columnSet[columnName]; found {
				if *columnChunk.MetaData.DictionaryPageOffset != 0 {
					//	we are dealing with a dictionary
					requestedColumns = append(requestedColumns,
						RequestedColumn{
							ColumnName: columnName,
							Start:      *columnMetaData.DictionaryPageOffset,
							End:        *columnMetaData.DictionaryPageOffset + columnMetaData.TotalCompressedSize - 1,
						})
				} else {
					//	we are not dealing with a dictionary
					requestedColumns = append(requestedColumns,
						RequestedColumn{
							ColumnName: columnName,
							Start:      columnChunk.FileOffset,
							End:        columnChunk.FileOffset + columnMetaData.TotalCompressedSize - 1,
						})
				}
			}
		}
	}

	return requestedColumns, nil
}
