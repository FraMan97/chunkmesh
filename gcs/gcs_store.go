package gcs

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

	"cloud.google.com/go/storage"
	"github.com/FraMan97/chunkmesh/internal/database"
	"github.com/FraMan97/chunkmesh/internal/models"
	"github.com/FraMan97/chunkmesh/internal/utils"
	"github.com/FraMan97/chunkmesh/pkg"
	"github.com/boltdb/bolt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GCSStorageOptions struct {
	BucketChunksPath      string
	MetadataDirectoryPath string
	Bucket                string
	Region                string
	AvgChunkSize          int
	ProjectId             string
	Endpoint              string
}

type GCSStorage struct {
	BucketChunksPath      string
	MetadataDirectoryPath string
	Bucket                string
	Client                *storage.Client
	Region                string
	ProjectId             string
	lock                  *sync.RWMutex
	boltdb                *bolt.DB
	AverageChunkSize      int
}

const (
	BucketFiles    = "files"
	BucketVersions = "versions"
	BucketChunks   = "chunks"
)

func NewGCSStorage(context context.Context, options *GCSStorageOptions) (*GCSStorage, error) {
	db, err := database.OpenDatabase(options.MetadataDirectoryPath)
	if err != nil {
		return nil, err
	}

	var clientOpts []option.ClientOption

	if options.Endpoint != "" {
		clientOpts = append(clientOpts, option.WithEndpoint(options.Endpoint))
		clientOpts = append(clientOpts, option.WithoutAuthentication())
	}

	client, err := storage.NewClient(context, clientOpts...)
	if err != nil {
		return nil, err
	}

	store := GCSStorage{
		BucketChunksPath:      options.BucketChunksPath,
		MetadataDirectoryPath: options.MetadataDirectoryPath,
		Bucket:                options.Bucket,
		Client:                client,
		boltdb:                db,
		lock:                  &sync.RWMutex{},
		AverageChunkSize:      options.AvgChunkSize,
	}

	buckets := []string{BucketFiles, BucketVersions, BucketChunks}
	for _, bucket := range buckets {
		err = database.EnsureBucket(store.boltdb, bucket)
		if err != nil {
			return nil, err
		}
	}
	client.Bucket(options.Bucket).Create(context, options.ProjectId, nil)
	return &store, nil
}

func (s *GCSStorage) AddByPath(context context.Context, fileName string, filePath string, options *pkg.StoreObjectOptions) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", filePath)
	}
	defer f.Close()

	saveToBucket := func(chunkId string, data []byte) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		wc := s.Client.Bucket(s.Bucket).Object(key).NewWriter(context)
		if _, err := io.Copy(wc, bytes.NewReader(data)); err != nil {
			wc.Close()
			return err
		}
		return wc.Close()
	}

	return utils.CoreAdd(s.boltdb, fileName, s.AverageChunkSize, options, f, saveToBucket)
}

func (s *GCSStorage) AddByInfo(context context.Context, fileName string, data []byte, options *pkg.StoreObjectOptions) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	r := bytes.NewReader(data)

	saveToBucket := func(chunkId string, data []byte) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		wc := s.Client.Bucket(s.Bucket).Object(key).NewWriter(context)
		if _, err := io.Copy(wc, bytes.NewReader(data)); err != nil {
			wc.Close()
			return err
		}
		return wc.Close()
	}

	return utils.CoreAdd(s.boltdb, fileName, s.AverageChunkSize, options, r, saveToBucket)
}

func (s *GCSStorage) Get(context context.Context, fileName string, versionIdRequested string, passphrase string, dst io.Writer) error {
	getFromBucket := func(chunkId string) ([]byte, error) {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		rc, err := s.Client.Bucket(s.Bucket).Object(key).NewReader(context)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		return io.ReadAll(rc)
	}
	return utils.CoreGet(s.boltdb, s.lock, fileName, versionIdRequested, passphrase, getFromBucket, dst)
}

func (s *GCSStorage) Delete(context context.Context, fileName string, versionId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleteChunk := func(chunkId string) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		err := s.Client.Bucket(s.Bucket).Object(key).Delete(context)
		if err != nil && err != storage.ErrObjectNotExist {
			return err
		}
		return nil
	}
	return utils.CoreDelete(s.boltdb, fileName, versionId, deleteChunk)
}

func (s *GCSStorage) GetLatestVersion(context context.Context, name string) (string, error) {
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

func (s *GCSStorage) CleanUp(context context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	listFn := func() ([]string, error) {
		var ids []string
		prefix := s.BucketChunksPath + "/"

		it := s.Client.Bucket(s.Bucket).Objects(context, &storage.Query{
			Prefix: prefix,
		})

		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}

			filename := filepath.Base(attrs.Name)
			chunkID := strings.TrimSuffix(filename, filepath.Ext(filename))
			ids = append(ids, chunkID)
		}
		return ids, nil
	}

	readFn := func(chunkId string) ([]byte, error) {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)

		rc, err := s.Client.Bucket(s.Bucket).Object(key).NewReader(context)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}

	deleteFn := func(chunkId string) error {
		key := fmt.Sprintf("%s/%s/%s/%s.chunk", s.BucketChunksPath, chunkId[:2], chunkId[2:4], chunkId)
		return s.Client.Bucket(s.Bucket).Object(key).Delete(context)
	}

	utils.CoreCleanUp(s.boltdb, listFn, readFn, deleteFn)
}

func (s *GCSStorage) BackupMetadata(context context.Context, bucketMetadata string, bucketMetadataPath string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	f, err := os.Open(filepath.Join(s.MetadataDirectoryPath, "boltdb.db"))
	if err != nil {
		return err
	}
	s.Client.Bucket(bucketMetadata).Create(context, s.ProjectId, nil)
	wc := s.Client.Bucket(bucketMetadata).Object(bucketMetadataPath).NewWriter(context)
	if _, err := io.Copy(wc, f); err != nil {
		wc.Close()
		return err
	}
	wc.Close()
	return nil
}

func (s *GCSStorage) Close() error {
	err := s.Client.Close()
	if err != nil {
		return err
	}
	err = s.boltdb.Close()
	if err != nil {
		return err
	}
	return nil
}
