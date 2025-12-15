package local

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
)

type LocalStorageOptions struct {
	ChunksPath   string
	MetadataURI  string
	MetadataType string
	AvgChunkSize int
}

type LocalStorage struct {
	ChunksPath       string
	MetadataURI      string
	metadataStorage  models.MetadataStore
	lock             *sync.RWMutex
	AverageChunkSize int
}

const (
	CollectionFiles    = "files"
	CollectionVersions = "versions"
	CollectionChunks   = "chunks"
)

var MetadataTypes = []string{"boltdb", "mongodb"}

func NewLocalStorage(context context.Context, options *LocalStorageOptions) (models.Store, error) {
	os.MkdirAll(options.MetadataURI, 0755)
	os.MkdirAll(filepath.Join(options.ChunksPath, "chunks"), 0755)

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

	storage := LocalStorage{
		MetadataURI:      options.MetadataURI,
		ChunksPath:       options.ChunksPath,
		metadataStorage:  db,
		lock:             &sync.RWMutex{},
		AverageChunkSize: options.AvgChunkSize,
	}

	collections := []string{CollectionFiles, CollectionVersions, CollectionChunks}
	for _, collection := range collections {
		err := storage.metadataStorage.EnsureCollection(context, collection)
		if err != nil {
			return nil, err
		}
	}

	return &storage, nil
}

func (ls *LocalStorage) AddByPath(context context.Context, fileName string, filePath string, options *pkg.StoreObjectOptions) (string, error) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", filePath)
	}
	defer f.Close()
	saveToDisk := func(chunkId string, data []byte) error {
		dirPath := filepath.Join(ls.ChunksPath, "chunks", chunkId[:2], chunkId[2:4])
		os.MkdirAll(dirPath, 0755)
		chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")
		return os.WriteFile(chunkFilePath, data, 0644)
	}
	return utils.CoreAdd(context, ls.metadataStorage, fileName, ls.AverageChunkSize, options, f, saveToDisk)
}

func (ls *LocalStorage) AddByInfo(context context.Context, fileName string, data []byte, options *pkg.StoreObjectOptions) (string, error) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	r := bytes.NewReader(data)

	saveToDisk := func(chunkId string, data []byte) error {
		dirPath := filepath.Join(ls.ChunksPath, "chunks", chunkId[:2], chunkId[2:4])
		os.MkdirAll(dirPath, 0755)
		chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")
		return os.WriteFile(chunkFilePath, data, 0644)
	}
	return utils.CoreAdd(context, ls.metadataStorage, fileName, ls.AverageChunkSize, options, r, saveToDisk)
}

func (ls *LocalStorage) Get(context context.Context, fileName string, versionIdRequested string, passphrase string, dst io.Writer) error {
	getFromLocal := func(chunkId string) ([]byte, error) {
		chunkPath := filepath.Join(ls.ChunksPath, "chunks", chunkId[:2], chunkId[2:4], chunkId+".chunk")
		chunkBytes, err := os.ReadFile(chunkPath)
		if err != nil {
			return nil, err
		}
		return chunkBytes, nil
	}
	return utils.CoreGet(context, ls.metadataStorage, ls.lock, fileName, versionIdRequested, passphrase, getFromLocal, dst)
}

func (ls *LocalStorage) Delete(context context.Context, fileName string, versionId string) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	deleteChunk := func(chunkId string) error {
		chunkFilePath := filepath.Join(ls.ChunksPath, "chunks", chunkId[:2], chunkId[2:4], chunkId+".chunk")
		err := os.Remove(chunkFilePath)
		if err != nil {
			return err
		}
		return nil
	}
	return utils.CoreDelete(context, ls.metadataStorage, fileName, versionId, deleteChunk)
}

func (ls *LocalStorage) pruneEmptyDirectoriesUnsafe() {
	chunksBaseDir := filepath.Join(ls.ChunksPath, "chunks")

	level1Dirs, err := os.ReadDir(chunksBaseDir)
	if err != nil {
		return
	}

	for _, l1 := range level1Dirs {
		if !l1.IsDir() {
			continue
		}
		l1Path := filepath.Join(chunksBaseDir, l1.Name())

		level2Dirs, err := os.ReadDir(l1Path)
		if err == nil {
			for _, l2 := range level2Dirs {
				if !l2.IsDir() {
					continue
				}
				l2Path := filepath.Join(l1Path, l2.Name())
				os.Remove(l2Path)
			}
		}
		os.Remove(l1Path)
	}
}

func (ls *LocalStorage) GetLatestVersion(context context.Context, name string) (string, error) {
	ls.lock.RLock()
	defer ls.lock.RUnlock()
	fileData, err := ls.metadataStorage.GetData(context, CollectionFiles, name)
	if err != nil {
		return "", err
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)
	return targetFile.LastVersion, nil
}

func (ls *LocalStorage) CleanUp(context context.Context) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	listFn := func() ([]string, error) {
		physicalFiles := utils.ListFiles(filepath.Join(ls.ChunksPath, "chunks"))
		var ids []string
		for _, fPath := range physicalFiles {
			fileNameWithExt := filepath.Base(fPath)
			chunkID := strings.TrimSuffix(fileNameWithExt, filepath.Ext(fileNameWithExt))
			ids = append(ids, chunkID)
		}
		return ids, nil
	}

	readFn := func(chunkId string) ([]byte, error) {
		chunkPath := filepath.Join(ls.ChunksPath, "chunks", chunkId[:2], chunkId[2:4], chunkId+".chunk")
		return os.ReadFile(chunkPath)
	}

	deleteFn := func(chunkId string) error {
		chunkPath := filepath.Join(ls.ChunksPath, "chunks", chunkId[:2], chunkId[2:4], chunkId+".chunk")
		return os.Remove(chunkPath)
	}

	utils.CoreCleanUp(context, ls.metadataStorage, listFn, readFn, deleteFn)

	ls.pruneEmptyDirectoriesUnsafe()
}

func (ls *LocalStorage) Close(context context.Context) error {
	return ls.metadataStorage.Close(context)
}
