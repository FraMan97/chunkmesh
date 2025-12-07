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

	"github.com/FraMan97/chunkmesh/internal/database"
	"github.com/FraMan97/chunkmesh/internal/models"
	"github.com/FraMan97/chunkmesh/internal/utils"
	"github.com/FraMan97/chunkmesh/pkg"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/boltdb/bolt"
)

type S3StorageOptions struct {
	BucketChunksPath      string
	MetadataDirectoryPath string
	AWSEndpoint           string
	Bucket                string
	Region                string
	AvgChunkSize          int
}

type S3Storage struct {
	BucketChunksPath      string
	MetadataDirectoryPath string
	Endpoint              string
	Bucket                string
	Region                string
	Client                *s3.Client
	lock                  *sync.RWMutex
	boltdb                *bolt.DB
	AverageChunkSize      int
}

const (
	BucketFiles    = "files"
	BucketVersions = "versions"
	BucketChunks   = "chunks"
)

func NewS3Storage(context context.Context, options *S3StorageOptions) (*S3Storage, error) {

	db, err := database.OpenDatabase(options.MetadataDirectoryPath)
	if err != nil {
		return nil, err
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
		BucketChunksPath:      options.BucketChunksPath,
		MetadataDirectoryPath: options.MetadataDirectoryPath,
		Endpoint:              options.AWSEndpoint,
		Bucket:                options.Bucket,
		Region:                options.Region,
		Client:                client,
		boltdb:                db,
		lock:                  &sync.RWMutex{},
		AverageChunkSize:      options.AvgChunkSize,
	}

	storage.Client.CreateBucket(context, &s3.CreateBucketInput{
		Bucket: aws.String(options.Bucket),
	})

	buckets := []string{BucketFiles, BucketVersions, BucketChunks}
	for _, bucket := range buckets {
		err = database.EnsureBucket(storage.boltdb, bucket)
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
	return utils.CoreAdd(s.boltdb, fileName, s.AverageChunkSize, options, f, saveToBucket)
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
	return utils.CoreAdd(s.boltdb, fileName, s.AverageChunkSize, options, r, saveToBucket)
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
	return utils.CoreGet(s.boltdb, s.lock, fileName, versionIdRequested, passphrase, getFromBucket, dst)
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
	return utils.CoreDelete(s.boltdb, fileName, versionId, deleteChunk)
}

func (s *S3Storage) GetLatestVersion(context context.Context, name string) (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	fileData, err := database.GetData(s.boltdb, BucketFiles, name)
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

	utils.CoreCleanUp(s.boltdb, listFn, readFn, deleteFn)
}

func (s *S3Storage) BackupMetadata(context context.Context, bucketMetadata string, bucketMetadataPath string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.Client.CreateBucket(context, &s3.CreateBucketInput{
		Bucket: aws.String(bucketMetadata),
	})
	f, err := os.Open(filepath.Join(s.MetadataDirectoryPath, "boltdb.db"))
	if err != nil {
		return err
	}
	_, err = s.Client.PutObject(context, &s3.PutObjectInput{
		Bucket: aws.String(bucketMetadata),
		Key:    aws.String(bucketMetadataPath),
		Body:   f,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Storage) Close() error {
	return s.boltdb.Close()
}
