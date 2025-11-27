package core

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

	"github.com/FraMan97/chunkmesh/src/database"
	"github.com/FraMan97/chunkmesh/src/models"
	"github.com/FraMan97/chunkmesh/src/utils"
	"github.com/boltdb/bolt"
	"github.com/google/uuid"
)

const (
	BucketFiles    = "files"
	BucketVersions = "versions"
	BucketChunks   = "chunks"
)

type chunkmesh struct {
	Path      string
	BoltDB    *bolt.DB
	Lock      *sync.RWMutex
	ChunkSize int
}

func NewChunkMeshStorage(path string, chunkSize int) (*chunkmesh, error) {
	os.MkdirAll(path, 0755)
	os.MkdirAll(filepath.Join(path, "chunks"), 0755)

	db, err := database.OpenDatabase(path)
	if err != nil {
		return nil, err
	}

	cm := chunkmesh{
		Path:      path,
		BoltDB:    db,
		Lock:      &sync.RWMutex{},
		ChunkSize: chunkSize,
	}

	buckets := []string{BucketFiles, BucketVersions, BucketChunks}
	for _, bucket := range buckets {
		err = database.EnsureBucket(cm.BoltDB, bucket)
		if err != nil {
			return nil, err
		}
	}

	return &cm, nil
}

func (cm *chunkmesh) AddByPath(name string, path string, compress bool) (string, error) {
	cm.Lock.Lock()
	defer cm.Lock.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", path)
	}
	defer f.Close()

	return add(cm, name, compress, f)
}

func (cm *chunkmesh) AddByInfo(name string, data []byte, compress bool) (string, error) {
	cm.Lock.Lock()
	defer cm.Lock.Unlock()

	r := bytes.NewReader(data)
	return add(cm, name, compress, r)
}

func add(cm *chunkmesh, name string, compression bool, reader io.Reader) (string, error) {
	var currentFile models.File
	fileData, err := database.GetData(cm.BoltDB, BucketFiles, name)

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

	totalRead, err := utils.ProcessChunks(reader, cm.ChunkSize, compression, runtime.NumCPU(), func(index int, c []byte) error {
		chunkId := utils.GenerateHash(c)

		mu.Lock()
		defer mu.Unlock()

		var currentChunk models.Chunk
		chunkData, err := database.GetData(cm.BoltDB, BucketChunks, chunkId)
		exists := (err == nil && chunkData != nil)

		if !exists {
			dirPath := filepath.Join(cm.Path, "chunks", chunkId[:2], chunkId[2:4])
			os.MkdirAll(dirPath, 0755)
			chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")

			if err := os.WriteFile(chunkFilePath, c, 0644); err != nil {
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
		if err := database.PutData(cm.BoltDB, BucketChunks, chunkId, chunkBytes); err != nil {
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
	if err := database.PutData(cm.BoltDB, BucketVersions, versionId, versionBytes); err != nil {
		return "", err
	}

	fileMetaBytes, err := json.Marshal(currentFile)
	if err != nil {
		return "", err
	}
	if err := database.PutData(cm.BoltDB, BucketFiles, name, fileMetaBytes); err != nil {
		return "", err
	}

	return versionId, nil
}

func (cm *chunkmesh) Get(name string, versionIdRequested string) ([]byte, error) {
	cm.Lock.RLock()
	defer cm.Lock.RUnlock()

	fileData, err := database.GetData(cm.BoltDB, BucketFiles, name)
	if err != nil || fileData == nil {
		return nil, fmt.Errorf("file '%s' not found", name)
	}
	var targetFile models.File
	if err := json.Unmarshal(fileData, &targetFile); err != nil {
		return nil, err
	}

	var finalVersionID string
	if versionIdRequested == "latest" {
		finalVersionID = targetFile.LastVersion
	} else {
		finalVersionID = versionIdRequested
	}

	versionData, err := database.GetData(cm.BoltDB, BucketVersions, finalVersionID)
	if err != nil || versionData == nil {
		return nil, fmt.Errorf("version '%s' not found", finalVersionID)
	}
	var targetVersion models.Version
	if err := json.Unmarshal(versionData, &targetVersion); err != nil {
		return nil, err
	}

	resultedFile := make([]byte, 0, targetVersion.Size)

	for _, chunkID := range targetVersion.Chunks {
		chunkMetaBytes, err := database.GetData(cm.BoltDB, BucketChunks, chunkID)
		if err != nil || chunkMetaBytes == nil {
			return nil, fmt.Errorf("chunk '%s' not found in DB", chunkID)
		}
		var chunkMeta models.Chunk
		if err := json.Unmarshal(chunkMetaBytes, &chunkMeta); err != nil {
			return nil, err
		}

		chunkPath := filepath.Join(cm.Path, "chunks", chunkID[:2], chunkID[2:4], chunkID+".chunk")
		chunkBytes, err := os.ReadFile(chunkPath)
		if err != nil {
			return nil, err
		}

		if chunkID != utils.GenerateHash(chunkBytes) {
			return nil, fmt.Errorf("integrity check failed for chunk '%s'", chunkID)
		}

		if chunkMeta.Compression {
			chunkBytes, err = utils.Decompress(chunkBytes)
			if err != nil {
				return nil, err
			}
		}
		resultedFile = append(resultedFile, chunkBytes...)
	}

	return resultedFile, nil
}

func (cm *chunkmesh) Delete(name string, versionId string) error {
	cm.Lock.Lock()
	defer cm.Lock.Unlock()
	return DeleteVersion(cm, name, versionId)
}

func DeleteVersion(cm *chunkmesh, name string, versionId string) error {
	fileData, err := database.GetData(cm.BoltDB, BucketFiles, name)
	if err != nil {
		return fmt.Errorf("file '%s' not found", name)
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)

	idVersion := versionId
	if versionId == "latest" {
		idVersion = targetFile.LastVersion
	}

	vData, err := database.GetData(cm.BoltDB, BucketVersions, idVersion)
	if err != nil {
		return fmt.Errorf("version '%s' not found", idVersion)
	}
	var targetVersion models.Version
	json.Unmarshal(vData, &targetVersion)

	for _, chunkID := range targetVersion.Chunks {
		cData, err := database.GetData(cm.BoltDB, BucketChunks, chunkID)
		if err != nil {
			continue
		}
		var chunkMeta models.Chunk
		json.Unmarshal(cData, &chunkMeta)

		chunkMeta.RefCount--

		if chunkMeta.RefCount <= 0 {
			chunkFilePath := filepath.Join(cm.Path, "chunks", chunkMeta.Id[:2], chunkMeta.Id[2:4], chunkMeta.Id+".chunk")
			os.Remove(chunkFilePath)
			database.DeleteKey(cm.BoltDB, BucketChunks, chunkID)
		} else {
			updatedCData, _ := json.Marshal(chunkMeta)
			database.PutData(cm.BoltDB, BucketChunks, chunkID, updatedCData)
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
		database.DeleteKey(cm.BoltDB, BucketFiles, name)
	} else {
		updatedFData, _ := json.Marshal(targetFile)
		database.PutData(cm.BoltDB, BucketFiles, name, updatedFData)
	}

	database.DeleteKey(cm.BoltDB, BucketVersions, idVersion)

	return nil
}

func (cm *chunkmesh) CleanOrphanChunks() {
	physicalFiles := utils.ListFiles(filepath.Join(cm.Path, "chunks"))

	cm.Lock.Lock()
	defer cm.Lock.Unlock()

	var filesToDelete []string

	for _, fPath := range physicalFiles {
		fileNameWithExt := filepath.Base(fPath)
		parts := strings.Split(fileNameWithExt, ".")
		if len(parts) != 2 {
			filesToDelete = append(filesToDelete, fPath)
			continue
		}
		chunkID := parts[0]

		exists, _ := database.ExistsKey(cm.BoltDB, BucketChunks, chunkID)
		if !exists {
			filesToDelete = append(filesToDelete, fPath)
		} else {
			cData, _ := database.GetData(cm.BoltDB, BucketChunks, chunkID)
			var c models.Chunk
			json.Unmarshal(cData, &c)
			if c.RefCount <= 0 {
				filesToDelete = append(filesToDelete, fPath)
				database.DeleteKey(cm.BoltDB, BucketChunks, chunkID)
			}
		}
	}

	if len(filesToDelete) == 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())

	for _, f := range filesToDelete {
		wg.Add(1)
		go func(filePath string) {
			sem <- struct{}{}
			defer func() {
				<-sem
				wg.Done()
			}()
			os.Remove(filePath)
		}(f)
	}
	wg.Wait()
}

func (chunkmesh *chunkmesh) GetLatestVersion(name string) (string, error) {
	chunkmesh.Lock.RLock()
	defer chunkmesh.Lock.RUnlock()
	fileData, err := database.GetData(chunkmesh.BoltDB, BucketFiles, name)
	if err != nil {
		return "", err
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)
	return targetFile.LastVersion, nil
}
