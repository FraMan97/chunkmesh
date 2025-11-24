package chunkmesh

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

type chunkmesh struct {
	Path      string
	Lock      *sync.Mutex
	ChunkSize int
	Files     map[string]file
	Chunks    map[string]chunk
	Versions  map[string]version
}

func NewChunkMeshStorage(path string, chunkSize int) (*chunkmesh, error) {
	os.MkdirAll(path, 0755)
	os.MkdirAll(filepath.Join(path, "chunks"), 0755)
	filePath := filepath.Join(path, "chunkmesh.json")
	chunkmesh := chunkmesh{
		Path:      path,
		Lock:      &sync.Mutex{},
		ChunkSize: chunkSize,
		Files:     make(map[string]file),
		Chunks:    make(map[string]chunk),
		Versions:  make(map[string]version),
	}
	_, err := os.Stat(filePath)
	if err == nil {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open the file 'chunkmesh.json'")
		}
		json.NewDecoder(bytes.NewBuffer(content)).Decode(&chunkmesh)
	}
	return &chunkmesh, nil
}

func (chunkmesh *chunkmesh) AddByPath(name string, path string) (string, error) {
	chunkmesh.Lock.Lock()
	defer chunkmesh.Lock.Unlock()
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", path)
	}
	newVersion, err := add(chunkmesh, name, content)
	if err != nil {
		return "", err
	}
	err = chunkmesh.save()
	if err != nil {
		return "", err
	}

	return newVersion, nil
}

func (chunkmesh *chunkmesh) AddByInfo(name string, data []byte) (string, error) {
	chunkmesh.Lock.Lock()
	defer chunkmesh.Lock.Unlock()
	newVersion, err := add(chunkmesh, name, data)
	if err != nil {
		return "", err
	}
	err = chunkmesh.save()
	if err != nil {
		return "", err
	}

	return newVersion, nil
}

func add(chunkmesh *chunkmesh, name string, content []byte) (string, error) {
	existingFile, ok := chunkmesh.Files[name]
	versionId := uuid.New().String()
	newFile := file{
		Name:        name,
		LastVersion: versionId,
		Versions:    []string{versionId},
	}
	newVersion := version{
		Id:          versionId,
		FileName:    name,
		Size:        len(content),
		Chunks:      []string{},
		PrevVersion: "",
	}
	chunkmesh.Versions[versionId] = newVersion

	if !ok {
		chunkmesh.Files[name] = newFile
	} else {
		existingFile.Versions = append(existingFile.Versions, newVersion.Id)
		newVersion.PrevVersion = existingFile.LastVersion
		existingFile.LastVersion = versionId
		chunkmesh.Files[name] = existingFile
	}

	chunks, paddings, err := splitFileIntoChunks(content, chunkmesh.ChunkSize)
	if err != nil {
		return "", err
	}
	for i, c := range chunks {
		chunkId := generateHash(c)
		existingChunk, ok := chunkmesh.Chunks[chunkId]
		if !ok {
			f, err := os.Create(filepath.Join(chunkmesh.Path, "chunks", chunkId+".chunk"))
			if err != nil {
				return "", fmt.Errorf("failed to create chunk '%s' file", chunkId)
			}
			defer f.Close()
			_, err = f.Write(c)
			if err != nil {
				return "", fmt.Errorf("failed to write chunk '%s' file", chunkId)
			}

			newChunk := chunk{
				Id:                 chunkId,
				Padding:            paddings[i],
				AssociatedVersions: []string{versionId},
				RefCount:           1,
			}
			chunkmesh.Chunks[chunkId] = newChunk
		} else {
			existingChunk.RefCount++
			existingChunk.AssociatedVersions = append(existingChunk.AssociatedVersions, newVersion.Id)
			chunkmesh.Chunks[chunkId] = existingChunk
		}
		newVersion.Chunks = append(newVersion.Chunks, chunkId)
	}
	chunkmesh.Versions[newVersion.Id] = newVersion
	return newVersion.Id, nil
}

func (chunkmesh *chunkmesh) save() error {
	chunkmeshPersisted := chunkmeshPersisted{
		ChunckSize: chunkmesh.ChunkSize,
		Files:      chunkmesh.Files,
		Chunks:     chunkmesh.Chunks,
		Versions:   chunkmesh.Versions,
	}

	file, err := os.Create(filepath.Join(chunkmesh.Path, "chunkmesh.json"))
	if err != nil {
		return fmt.Errorf("failed to create the 'chunkmesh.json' file")
	}
	defer file.Close()
	content, err := json.Marshal(chunkmeshPersisted)
	if err != nil {
		return fmt.Errorf("failed to serialize 'chunkmesh' object")
	}
	_, err = file.Write(content)
	if err != nil {
		return fmt.Errorf("failed to write the 'chunkmesh.json' file")
	}
	return nil
}

func deleteVersion(chunkmesh *chunkmesh, name string, versionId string) error {
	targetFile, ok := chunkmesh.Files[name]
	if !ok {
		return fmt.Errorf("file '%s' not found", name)
	}

	var idVersion string
	if versionId == "latest" {
		idVersion = targetFile.LastVersion
	} else {
		idVersion = versionId
	}

	targetVersion, ok := chunkmesh.Versions[idVersion]
	if !ok {
		return fmt.Errorf("version '%s' not found for file '%s'", idVersion, name)
	}

	if targetVersion.PrevVersion != "" {
		if prevVersion, ok := chunkmesh.Versions[targetVersion.PrevVersion]; ok {
			chunkmesh.Versions[prevVersion.Id] = prevVersion
		}
	}

	for _, chunkID := range targetVersion.Chunks {
		chunk, chunkOk := chunkmesh.Chunks[chunkID]
		if !chunkOk {
			continue
		}

		chunk.RefCount--

		if chunk.RefCount <= 0 {
			chunkFilePath := filepath.Join(chunkmesh.Path, "chunks", chunk.Id+".chunk")
			if err := os.Remove(chunkFilePath); err != nil {
				return fmt.Errorf("failed to delete the chunk file '%s': %w", chunk.Id, err)
			}
			delete(chunkmesh.Chunks, chunkID)
		} else {
			chunkmesh.Chunks[chunkID] = chunk
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
		targetFile.LastVersion = targetVersion.PrevVersion
	}

	if len(targetFile.Versions) == 0 {
		delete(chunkmesh.Files, name)
	} else {
		chunkmesh.Files[name] = targetFile
	}

	delete(chunkmesh.Versions, idVersion)

	return nil
}

func (chunkmesh *chunkmesh) Delete(name string, versionId string) error {
	chunkmesh.Lock.Lock()
	defer chunkmesh.Lock.Unlock()
	err := deleteVersion(chunkmesh, name, versionId)
	if err != nil {
		return err
	}
	err = chunkmesh.save()
	if err != nil {
		return err
	}
	return nil
}
