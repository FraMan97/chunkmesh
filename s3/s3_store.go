package s3store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	boltdb_manager "github.com/FraMan97/chunkmesh/internal/database/boltdb"
	mongodb_manager "github.com/FraMan97/chunkmesh/internal/database/mongodb"
	"github.com/FraMan97/chunkmesh/internal/models"
	"github.com/FraMan97/chunkmesh/internal/utils"
	"github.com/FraMan97/chunkmesh/pkg"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3StorageOptions struct {
	BucketChunksPath string
	MetadataURI      string
	MetadataType     string
	AWSEndpoint      string
	Bucket           string
	Region           string
	AvgChunkSize     int
}

type S3Storage struct {
	BucketChunksPath string
	MetadataURI      string
	Endpoint         string
	Bucket           string
	Region           string
	Client           *s3.Client
	lock             *sync.RWMutex
	metadataStorage  models.MetadataStore
	AverageChunkSize int
}

const (
	CollectionFiles    = "files"
	CollectionVersions = "versions"
	CollectionChunks   = "chunks"
)

var MetadataTypes = []string{"boltdb", "mongodb"}

func NewS3Storage(context context.Context, options *S3StorageOptions) (*S3Storage, error) {

	var db models.MetadataStore
	var err error
	switch options.MetadataType {
	case "boltdb":
		db, err = boltdb_manager.OpenDatabase(options.MetadataURI)
		if err != nil {
			return nil, err
		}
	case "mongodb":
		db, err = mongodb_manager.OpenDatabase(options.MetadataURI)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("metadata type %s not supported. Supported only '%v'", options.MetadataType, MetadataTypes)
	}

	cfg, err := config.LoadDefaultConfig(context,
		config.WithRegion(options.Region),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if options.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(options.AWSEndpoint)
		}
		o.UsePathStyle = true
	})

	storage := S3Storage{
		BucketChunksPath: options.BucketChunksPath,
		MetadataURI:      options.MetadataURI,
		Endpoint:         options.AWSEndpoint,
		Bucket:           options.Bucket,
		Region:           options.Region,
		Client:           client,
		metadataStorage:  db,
		lock:             &sync.RWMutex{},
		AverageChunkSize: options.AvgChunkSize,
	}

	storage.Client.CreateBucket(context, &s3.CreateBucketInput{
		Bucket: aws.String(options.Bucket),
	})

	collections := []string{CollectionFiles, CollectionVersions, CollectionChunks}
	for _, collection := range collections {
		err := storage.metadataStorage.EnsureCollection(context, collection)
		if err != nil {
			return nil, err
		}
	}

	return &storage, nil
}

func (s *S3Storage) AddByPath(context context.Context, fileName string, filePath string, options *pkg.StoreObjectOptions) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", filePath)
	}
	defer f.Close()
	saveToBucket := func(chunkId string, data []byte) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		input := &s3.PutObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		}

		_, err := s.Client.PutObject(context, input)
		if err != nil {
			return err
		}
		return nil
	}
	return utils.CoreAdd(context, s.metadataStorage, fileName, s.AverageChunkSize, options, f, saveToBucket)
}

func (s *S3Storage) AddByInfo(context context.Context, fileName string, data []byte, options *pkg.StoreObjectOptions) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	r := bytes.NewReader(data)

	saveToBucket := func(chunkId string, data []byte) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		input := &s3.PutObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		}

		_, err := s.Client.PutObject(context, input)
		if err != nil {
			return err
		}
		return nil
	}
	return utils.CoreAdd(context, s.metadataStorage, fileName, s.AverageChunkSize, options, r, saveToBucket)
}

func (s *S3Storage) Get(context context.Context, fileName string, versionIdRequested string, passphrase string, dst io.Writer) error {
	getFromBucket := func(chunkId string) ([]byte, error) {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)
		input := &s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
		}
		obj, err := s.Client.GetObject(context, input)
		if err != nil {
			return nil, err
		}
		defer obj.Body.Close()

		data, err := io.ReadAll(obj.Body)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return utils.CoreGet(context, s.metadataStorage, s.lock, fileName, versionIdRequested, passphrase, getFromBucket, dst)
}

func (s *S3Storage) Delete(context context.Context, fileName string, versionId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	deleteChunk := func(chunkId string) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
		}
		_, err := s.Client.DeleteObject(context, input)
		if err != nil {
			return err
		}
		return nil
	}
	return utils.CoreDelete(context, s.metadataStorage, fileName, versionId, deleteChunk)
}

func (s *S3Storage) GetLatestVersion(context context.Context, name string) (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	fileData, err := s.metadataStorage.GetData(context, CollectionFiles, name)
	if err != nil {
		return "", err
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)
	return targetFile.LastVersion, nil
}

func (s *S3Storage) CleanUp(context context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	listFn := func() ([]string, error) {
		var ids []string
		prefix := s.BucketChunksPath + "/"
		paginator := s3.NewListObjectsV2Paginator(s.Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket),
			Prefix: aws.String(prefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context)
			if err != nil {
				return nil, err
			}
			for _, obj := range page.Contents {
				key := *obj.Key
				filename := filepath.Base(key)
				chunkID := strings.TrimSuffix(filename, filepath.Ext(filename))
				ids = append(ids, chunkID)
			}
		}
		return ids, nil
	}

	readFn := func(chunkId string) ([]byte, error) {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)
		input := &s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
		}
		obj, err := s.Client.GetObject(context, input)
		if err != nil {
			return nil, err
		}
		defer obj.Body.Close()
		return io.ReadAll(obj.Body)
	}

	deleteFn := func(chunkId string) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)
		_, err := s.Client.DeleteObject(context, &s3.DeleteObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
		})
		return err
	}

	utils.CoreCleanUp(context, s.metadataStorage, listFn, readFn, deleteFn)
}

func (s *S3Storage) BackupMetadata(context context.Context, bucketMetadata string, bucketMetadataPath string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.metadataStorage == nil || s.MetadataURI == "" || strings.HasPrefix(s.MetadataURI, "mongodb") {
		return fmt.Errorf("backup metadata is only supported for local BoltDB, not MongoDB")
	}

	s.Client.CreateBucket(context, &s3.CreateBucketInput{
		Bucket: aws.String(bucketMetadata),
	})

	f, err := os.Open(filepath.Join(s.MetadataURI, "boltdb.db"))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = s.Client.PutObject(context, &s3.PutObjectInput{
		Bucket: aws.String(bucketMetadata),
		Key:    aws.String(bucketMetadataPath),
		Body:   f,
	})
	return err
}

func (s *S3Storage) Close(context context.Context) error {
	return s.metadataStorage.Close(context)
}
