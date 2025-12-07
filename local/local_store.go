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
	"time"

	"github.com/FraMan97/chunkmesh/internal/database"
	"github.com/FraMan97/chunkmesh/internal/models"
	"github.com/FraMan97/chunkmesh/internal/utils"
	"github.com/FraMan97/chunkmesh/pkg"
	"github.com/boltdb/bolt"
)

type LocalStorageOptions struct {
	ChunksPath            string
	MetadataDirectoryPath string
	AvgChunkSize          int
}

type LocalStorage struct {
	ChunksPath            string
	MetadataDirectoryPath string
	boltdb                *bolt.DB
	lock                  *sync.RWMutex
	AverageChunkSize      int
}

const (
	BucketFiles    = "files"
	BucketVersions = "versions"
	BucketChunks   = "chunks"
)

func NewLocalStorage(context context.Context, options *LocalStorageOptions) (models.Store, error) {
	os.MkdirAll(options.MetadataDirectoryPath, 0755)
	os.MkdirAll(filepath.Join(options.MetadataDirectoryPath, "chunks"), 0755)

	db, err := database.OpenDatabase(options.MetadataDirectoryPath)
	if err != nil {
		return nil, err
	}

	ls := LocalStorage{
		MetadataDirectoryPath: options.MetadataDirectoryPath,
		ChunksPath:            options.ChunksPath,
		boltdb:                db,
		lock:                  &sync.RWMutex{},
		AverageChunkSize:      options.AvgChunkSize,
	}

	buckets := []string{BucketFiles, BucketVersions, BucketChunks}
	for _, bucket := range buckets {
		err = database.EnsureBucket(ls.boltdb, bucket)
		if err != nil {
			return nil, err
		}
	}

	return &ls, nil
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
		dirPath := filepath.Join(ls.ChunksPath, chunkId[:2], chunkId[2:4])
		os.MkdirAll(dirPath, 0755)
		chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")
		return os.WriteFile(chunkFilePath, data, 0644)
	}
	return utils.CoreAdd(ls.boltdb, fileName, ls.AverageChunkSize, options, f, saveToDisk)
}

func (ls *LocalStorage) AddByInfo(context context.Context, fileName string, data []byte, options *pkg.StoreObjectOptions) (string, error) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	r := bytes.NewReader(data)

	saveToDisk := func(chunkId string, data []byte) error {
		dirPath := filepath.Join(ls.ChunksPath, chunkId[:2], chunkId[2:4])
		os.MkdirAll(dirPath, 0755)
		chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")
		return os.WriteFile(chunkFilePath, data, 0644)
	}
	return utils.CoreAdd(ls.boltdb, fileName, ls.AverageChunkSize, options, r, saveToDisk)
}

func (ls *LocalStorage) Get(context context.Context, fileName string, versionIdRequested string, passphrase string, dst io.Writer) error {
	getFromLocal := func(chunkId string) ([]byte, error) {
		chunkPath := filepath.Join(ls.ChunksPath, chunkId[:2], chunkId[2:4], chunkId+".chunk")
		chunkBytes, err := os.ReadFile(chunkPath)
		if err != nil {
			return nil, err
		}
		return chunkBytes, nil
	}
	return utils.CoreGet(ls.boltdb, ls.lock, fileName, versionIdRequested, passphrase, getFromLocal, dst)
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
	return utils.CoreDelete(ls.boltdb, fileName, versionId, deleteChunk)
}
func (ls *LocalStorage) cleanOrphanChunksUnsafe() {
	physicalFiles := utils.ListFiles(filepath.Join(ls.ChunksPath, "chunks"))

	for _, fPath := range physicalFiles {
		fileNameWithExt := filepath.Base(fPath)
		parts := strings.Split(fileNameWithExt, ".")
		if len(parts) != 2 {
			os.Remove(fPath)
			continue
		}
		chunkID := parts[0]

		exists, _ := database.ExistsKey(ls.boltdb, BucketChunks, chunkID)
		if !exists {
			os.Remove(fPath)
		} else {
			cData, _ := database.GetData(ls.boltdb, BucketChunks, chunkID)
			var c models.Chunk
			json.Unmarshal(cData, &c)
			if c.RefCount <= 0 {
				os.Remove(fPath)
				database.DeleteKey(ls.boltdb, BucketChunks, chunkID)
			}
		}
	}
}

func (ls *LocalStorage) cleanCorruptedVersionsUnsafe() {
	vData, err := database.GetAllData(ls.boltdb, BucketVersions)
	if err != nil {
		return
	}

	for _, dataBytes := range vData {
		var v models.Version
		if err := json.Unmarshal(dataBytes, &v); err != nil {
			continue
		}
		corrupted := false

		for _, chunkID := range v.Chunks {
			chunkPath := filepath.Join(ls.ChunksPath, "chunks", chunkID[:2], chunkID[2:4], chunkID+".chunk")
			file, err := os.ReadFile(chunkPath)
			if err != nil {
				corrupted = true
				break
			}
			hash := utils.GenerateHash(file)
			if hash != chunkID {
				corrupted = true
				break
			}
		}

		if corrupted {
			ls.deleteVersionUnsafe(v.FileName, v.Id)
		}
	}
}

func (ls *LocalStorage) cleanExpiredVersionsUnsafe() {
	vData, err := database.GetAllData(ls.boltdb, BucketVersions)
	if err != nil {
		return
	}

	now := time.Now().UTC()

	for _, dataBytes := range vData {
		var v models.Version
		if err := json.Unmarshal(dataBytes, &v); err != nil {
			continue
		}

		if v.Retention <= 0 {
			continue
		}

		expirationTime := v.CreatedAt.Add(time.Duration(v.Retention) * time.Second)

		if expirationTime.Before(now) {
			ls.deleteVersionUnsafe(v.FileName, v.Id)
		}
	}
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

func (ls *LocalStorage) deleteVersionUnsafe(name string, versionId string) error {
	fileData, err := database.GetData(ls.boltdb, BucketFiles, name)
	if err != nil {
		return fmt.Errorf("file '%s' not found", name)
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)

	idVersion := versionId
	if versionId == "latest" {
		idVersion = targetFile.LastVersion
	}

	vData, err := database.GetData(ls.boltdb, BucketVersions, idVersion)
	if err != nil {
		return fmt.Errorf("version '%s' not found", idVersion)
	}
	var targetVersion models.Version
	json.Unmarshal(vData, &targetVersion)

	for _, chunkID := range targetVersion.Chunks {
		cData, err := database.GetData(ls.boltdb, BucketChunks, chunkID)
		if err != nil {
			continue
		}
		var chunkMeta models.Chunk
		json.Unmarshal(cData, &chunkMeta)

		chunkMeta.RefCount--

		if chunkMeta.RefCount <= 0 {
			chunkFilePath := filepath.Join(ls.ChunksPath, "chunks", chunkMeta.Id[:2], chunkMeta.Id[2:4], chunkMeta.Id+".chunk")
			os.Remove(chunkFilePath)
			database.DeleteKey(ls.boltdb, BucketChunks, chunkID)
		} else {
			updatedCData, _ := json.Marshal(chunkMeta)
			database.PutData(ls.boltdb, BucketChunks, chunkID, updatedCData)
		}
	}

	var newVersions []string
	for _, vID := range targetFile.Versions {
		if vID != idVersion {
			newVersions = append(newVersions, vID)
		}
	}
	targetFile.Versions = newVersions

	if targetFile.LastVersion == idVersion {
		if len(newVersions) > 0 {
			targetFile.LastVersion = newVersions[len(newVersions)-1]
		} else {
			targetFile.LastVersion = ""
		}
	}

	if len(targetFile.Versions) == 0 {
		database.DeleteKey(ls.boltdb, BucketFiles, name)
	} else {
		updatedFData, _ := json.Marshal(targetFile)
		database.PutData(ls.boltdb, BucketFiles, name, updatedFData)
	}

	database.DeleteKey(ls.boltdb, BucketVersions, idVersion)

	return nil
}

func (ls *LocalStorage) GetLatestVersion(context context.Context, name string) (string, error) {
	ls.lock.RLock()
	defer ls.lock.RUnlock()
	fileData, err := database.GetData(ls.boltdb, BucketFiles, name)
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

	utils.CoreCleanUp(ls.boltdb, listFn, readFn, deleteFn)

	ls.pruneEmptyDirectoriesUnsafe()
}
