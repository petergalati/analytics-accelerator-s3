package service

import (
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

type CacheService struct {
	elasticacheClient *elasticache.Client
}
