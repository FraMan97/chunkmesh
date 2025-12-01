package chunkmesh

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/FraMan97/chunkmesh/internal/database"
	"github.com/FraMan97/chunkmesh/internal/models"
	"github.com/FraMan97/chunkmesh/internal/utils"
	"github.com/boltdb/bolt"
	"github.com/google/uuid"
)

const (
	BucketFiles    = "files"
	BucketVersions = "versions"
	BucketChunks   = "chunks"
)

type chunkmesh struct {
	Path             string
	boltdb           *bolt.DB
	lock             *sync.RWMutex
	AverageChunkSize int
}

func NewChunkMeshStorage(path string, avgChunkSize int) (*chunkmesh, error) {
	os.MkdirAll(path, 0755)
	os.MkdirAll(filepath.Join(path, "chunks"), 0755)

	db, err := database.OpenDatabase(path)
	if err != nil {
		return nil, err
	}

	cm := chunkmesh{
		Path:             path,
		boltdb:           db,
		lock:             &sync.RWMutex{},
		AverageChunkSize: avgChunkSize,
	}

	buckets := []string{BucketFiles, BucketVersions, BucketChunks}
	for _, bucket := range buckets {
		err = database.EnsureBucket(cm.boltdb, bucket)
		if err != nil {
			return nil, err
		}
	}

	return &cm, nil
}

func (cm *chunkmesh) AddByPath(name string, path string, compress bool, retention int, passphrase string) (string, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", path)
	}
	defer f.Close()

	return add(cm, name, compress, retention, passphrase, f)
}

func (cm *chunkmesh) AddByInfo(name string, data []byte, compress bool, retention int, passphrase string) (string, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	r := bytes.NewReader(data)
	return add(cm, name, compress, retention, passphrase, r)
}

func add(cm *chunkmesh, name string, compression bool, retention int, passphrase string, reader io.Reader) (string, error) {
	var currentFile models.File
	fileData, err := database.GetData(cm.boltdb, BucketFiles, name)

	versionId := uuid.New().String()

	if err != nil || fileData == nil {
		currentFile = models.File{
			Name:        name,
			LastVersion: versionId,
			Versions:    []string{versionId},
		}
	} else {
		if err := json.Unmarshal(fileData, &currentFile); err != nil {
			return "", err
		}
		currentFile.Versions = append(currentFile.Versions, versionId)
		currentFile.LastVersion = versionId
	}

	newVersion := models.Version{
		Id:          versionId,
		Retention:   retention,
		CreatedAt:   time.Now().UTC(),
		FileName:    name,
		Size:        0,
		Chunks:      []string{},
		PrevVersion: "",
	}

	if len(currentFile.Versions) > 1 {
		newVersion.PrevVersion = currentFile.Versions[len(currentFile.Versions)-2]
	}

	tempChunks := make(map[int]string)
	var mu sync.Mutex

	totalRead, err := utils.ProcessChunks(reader, cm.AverageChunkSize, compression, runtime.NumCPU(), func(index int, c []byte) error {
		mu.Lock()
		defer mu.Unlock()

		var processedData []byte
		if passphrase != "" {
			aesKey := utils.DeriveKey(passphrase)
			processedData, err = utils.Encrypt(c, aesKey)
			if err != nil {
				return err
			}
		} else {
			processedData = c
		}
		chunkId := utils.GenerateHash(processedData)

		var currentChunk models.Chunk
		chunkData, err := database.GetData(cm.boltdb, BucketChunks, chunkId)
		exists := (err == nil && chunkData != nil)

		if !exists {
			dirPath := filepath.Join(cm.Path, "chunks", chunkId[:2], chunkId[2:4])
			os.MkdirAll(dirPath, 0755)
			chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")

			if err := os.WriteFile(chunkFilePath, processedData, 0644); err != nil {
				return err
			}

			currentChunk = models.Chunk{
				Id:          chunkId,
				Compression: compression,
				RefCount:    1,
			}
		} else {
			if err := json.Unmarshal(chunkData, &currentChunk); err != nil {
				return err
			}
			currentChunk.RefCount++
		}

		chunkBytes, err := json.Marshal(currentChunk)
		if err != nil {
			return err
		}
		if err := database.PutData(cm.boltdb, BucketChunks, chunkId, chunkBytes); err != nil {
			return err
		}

		tempChunks[index] = chunkId
		return nil
	})

	if err != nil {
		return "", err
	}

	newVersion.Chunks = make([]string, len(tempChunks))
	for i := 0; i < len(tempChunks); i++ {
		if id, ok := tempChunks[i]; ok {
			newVersion.Chunks[i] = id
		} else {
			return "", fmt.Errorf("missing chunk index %d", i)
		}
	}

	newVersion.Size = totalRead

	versionBytes, err := json.Marshal(newVersion)
	if err != nil {
		return "", err
	}
	if err := database.PutData(cm.boltdb, BucketVersions, versionId, versionBytes); err != nil {
		return "", err
	}

	fileMetaBytes, err := json.Marshal(currentFile)
	if err != nil {
		return "", err
	}
	if err := database.PutData(cm.boltdb, BucketFiles, name, fileMetaBytes); err != nil {
		return "", err
	}

	return versionId, nil
}

func (cm *chunkmesh) Get(name string, versionIdRequested string, key string, dst io.Writer) error {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	fileData, err := database.GetData(cm.boltdb, BucketFiles, name)
	if err != nil || fileData == nil {
		return fmt.Errorf("file '%s' not found", name)
	}
	var targetFile models.File
	if err := json.Unmarshal(fileData, &targetFile); err != nil {
		return err
	}

	var finalVersionID string
	if versionIdRequested == "latest" {
		finalVersionID = targetFile.LastVersion
	} else {
		finalVersionID = versionIdRequested
	}

	versionData, err := database.GetData(cm.boltdb, BucketVersions, finalVersionID)
	if err != nil || versionData == nil {
		return fmt.Errorf("version '%s' not found", finalVersionID)
	}
	var targetVersion models.Version
	if err := json.Unmarshal(versionData, &targetVersion); err != nil {
		return err
	}

	resultedFile := make([]byte, 0, targetVersion.Size)

	for _, chunkID := range targetVersion.Chunks {
		chunkMetaBytes, err := database.GetData(cm.boltdb, BucketChunks, chunkID)
		if err != nil || chunkMetaBytes == nil {
			return fmt.Errorf("chunk '%s' not found in DB", chunkID)
		}
		var chunkMeta models.Chunk
		if err := json.Unmarshal(chunkMetaBytes, &chunkMeta); err != nil {
			return err
		}

		chunkPath := filepath.Join(cm.Path, "chunks", chunkID[:2], chunkID[2:4], chunkID+".chunk")
		chunkBytes, err := os.ReadFile(chunkPath)
		if err != nil {
			return err
		}

		if chunkID != utils.GenerateHash(chunkBytes) {
			return fmt.Errorf("integrity check failed for chunk '%s'", chunkID)
		}

		if key != "" {
			aesKey := utils.DeriveKey(key)
			chunkBytes, err = utils.Decrypt(chunkBytes, aesKey)
			if err != nil {
				return fmt.Errorf("decryption failed for chunk '%s': %v", chunkID, err)
			}
		}

		if chunkMeta.Compression {
			chunkBytes, err = utils.Decompress(chunkBytes)
			if err != nil {
				return err
			}
		}
		resultedFile = append(resultedFile, chunkBytes...)
	}

	dst.Write(resultedFile)
	return nil
}

func (cm *chunkmesh) CleanUp() {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.cleanOrphanChunksUnsafe()
	cm.cleanCorruptedVersionsUnsafe()
	cm.cleanExpiredVersionsUnsafe()
	cm.pruneEmptyDirectoriesUnsafe()
}

func (cm *chunkmesh) Delete(name string, versionId string) error {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	return cm.deleteVersionUnsafe(name, versionId)
}

func (cm *chunkmesh) cleanOrphanChunksUnsafe() {
	physicalFiles := utils.ListFiles(filepath.Join(cm.Path, "chunks"))

	for _, fPath := range physicalFiles {
		fileNameWithExt := filepath.Base(fPath)
		parts := strings.Split(fileNameWithExt, ".")
		if len(parts) != 2 {
			os.Remove(fPath)
			continue
		}
		chunkID := parts[0]

		exists, _ := database.ExistsKey(cm.boltdb, BucketChunks, chunkID)
		if !exists {
			os.Remove(fPath)
		} else {
			cData, _ := database.GetData(cm.boltdb, BucketChunks, chunkID)
			var c models.Chunk
			json.Unmarshal(cData, &c)
			if c.RefCount <= 0 {
				os.Remove(fPath)
				database.DeleteKey(cm.boltdb, BucketChunks, chunkID)
			}
		}
	}
}

func (cm *chunkmesh) cleanCorruptedVersionsUnsafe() {
	vData, err := database.GetAllData(cm.boltdb, BucketVersions)
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
			chunkPath := filepath.Join(cm.Path, "chunks", chunkID[:2], chunkID[2:4], chunkID+".chunk")
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
			cm.deleteVersionUnsafe(v.FileName, v.Id)
		}
	}
}

func (cm *chunkmesh) cleanExpiredVersionsUnsafe() {
	vData, err := database.GetAllData(cm.boltdb, BucketVersions)
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
			cm.deleteVersionUnsafe(v.FileName, v.Id)
		}
	}
}

func (cm *chunkmesh) pruneEmptyDirectoriesUnsafe() {
	chunksBaseDir := filepath.Join(cm.Path, "chunks")

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

func (cm *chunkmesh) deleteVersionUnsafe(name string, versionId string) error {
	fileData, err := database.GetData(cm.boltdb, BucketFiles, name)
	if err != nil {
		return fmt.Errorf("file '%s' not found", name)
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)

	idVersion := versionId
	if versionId == "latest" {
		idVersion = targetFile.LastVersion
	}

	vData, err := database.GetData(cm.boltdb, BucketVersions, idVersion)
	if err != nil {
		return fmt.Errorf("version '%s' not found", idVersion)
	}
	var targetVersion models.Version
	json.Unmarshal(vData, &targetVersion)

	for _, chunkID := range targetVersion.Chunks {
		cData, err := database.GetData(cm.boltdb, BucketChunks, chunkID)
		if err != nil {
			continue
		}
		var chunkMeta models.Chunk
		json.Unmarshal(cData, &chunkMeta)

		chunkMeta.RefCount--

		if chunkMeta.RefCount <= 0 {
			chunkFilePath := filepath.Join(cm.Path, "chunks", chunkMeta.Id[:2], chunkMeta.Id[2:4], chunkMeta.Id+".chunk")
			os.Remove(chunkFilePath)
			database.DeleteKey(cm.boltdb, BucketChunks, chunkID)
		} else {
			updatedCData, _ := json.Marshal(chunkMeta)
			database.PutData(cm.boltdb, BucketChunks, chunkID, updatedCData)
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
		database.DeleteKey(cm.boltdb, BucketFiles, name)
	} else {
		updatedFData, _ := json.Marshal(targetFile)
		database.PutData(cm.boltdb, BucketFiles, name, updatedFData)
	}

	database.DeleteKey(cm.boltdb, BucketVersions, idVersion)

	return nil
}

func (chunkmesh *chunkmesh) GetLatestVersion(name string) (string, error) {
	chunkmesh.lock.RLock()
	defer chunkmesh.lock.RUnlock()
	fileData, err := database.GetData(chunkmesh.boltdb, BucketFiles, name)
	if err != nil {
		return "", err
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)
	return targetFile.LastVersion, nil
}
