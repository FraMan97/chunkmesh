package chunkmesh

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open the file '%s'", path)
	}
	defer f.Close()

	newVersion, err := add(chunkmesh, name, f)
	if err != nil {
		return "", err
	}
	return newVersion, chunkmesh.save()
}

func (chunkmesh *chunkmesh) AddByInfo(name string, data []byte) (string, error) {
	chunkmesh.Lock.Lock()
	defer chunkmesh.Lock.Unlock()

	r := bytes.NewReader(data)
	newVersion, err := add(chunkmesh, name, r)
	if err != nil {
		return "", err
	}
	return newVersion, chunkmesh.save()
}

func add(chunkmesh *chunkmesh, name string, reader io.Reader) (string, error) {
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
		Size:        0,
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

	totalRead, err := ProcessChunks(reader, chunkmesh.ChunkSize, func(c []byte, padding int) error {
		chunkId := generateHash(c)
		existingChunk, ok := chunkmesh.Chunks[chunkId]

		if !ok {

			dirPath := filepath.Join(chunkmesh.Path, "chunks", chunkId[:2], chunkId[2:4])

			os.MkdirAll(dirPath, 0755)

			chunkFilePath := filepath.Join(dirPath, chunkId+".chunk")

			err := func() error {
				f, err := os.Create(chunkFilePath)
				if err != nil {
					return fmt.Errorf("failed to create the chunk '%s' file", chunkId)
				}
				defer f.Close()

				_, err = f.Write(c)
				if err != nil {
					return fmt.Errorf("failet to write the chunk '%s'", chunkId)
				}
				return nil
			}()

			if err != nil {
				return err
			}

			newChunk := chunk{
				Id:                 chunkId,
				Padding:            padding,
				AssociatedVersions: []string{versionId},
				RefCount:           1,
			}
			chunkmesh.Chunks[chunkId] = newChunk

		} else {
			existingChunk.RefCount++
			existingChunk.AssociatedVersions = append(existingChunk.AssociatedVersions, versionId)
			chunkmesh.Chunks[chunkId] = existingChunk
		}

		newVersion.Chunks = append(newVersion.Chunks, chunkId)
		return nil
	})

	if err != nil {
		return "", err
	}

	newVersion.Size = totalRead
	chunkmesh.Versions[newVersion.Id] = newVersion
	return newVersion.Id, nil
}

func (chunkmesh *chunkmesh) save() error {
	chunkmeshPersisted := chunkmeshPersisted{
		ChunkSize: chunkmesh.ChunkSize,
		Files:     chunkmesh.Files,
		Chunks:    chunkmesh.Chunks,
		Versions:  chunkmesh.Versions,
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
			chunkFilePath := filepath.Join(chunkmesh.Path, "chunks", chunk.Id[:2], chunk.Id[2:4], chunk.Id+".chunk")
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

func (chunkmesh *chunkmesh) Get(name string, version string) ([]byte, error) {
	chunkmesh.Lock.Lock()
	defer chunkmesh.Lock.Unlock()

	existingFile, ok := chunkmesh.Files[name]
	if !ok {
		return nil, fmt.Errorf("failed to find the file %s", name)
	}

	var versionID string
	if version == "latest" {
		versionID = existingFile.LastVersion
	} else {
		versionID = version
	}

	targetVersion, ok := chunkmesh.Versions[versionID]
	if !ok {
		return nil, fmt.Errorf("failed to find the version '%s'", versionID)
	}

	resultedFile := make([]byte, 0, targetVersion.Size)

	for _, chunkID := range targetVersion.Chunks {
		chunkInfo, ok := chunkmesh.Chunks[chunkID]
		if !ok {
			return nil, fmt.Errorf("metadata corruption: chunk '%s' not found", chunkID)
		}

		chunkPath := filepath.Join(chunkmesh.Path, "chunks", chunkID[:2], chunkID[2:4], chunkID+".chunk")

		chunkBytes, err := func() ([]byte, error) {
			f, err := os.Open(chunkPath)
			if err != nil {
				return nil, err
			}
			defer f.Close()

			return io.ReadAll(f)
		}()

		if err != nil {
			return nil, fmt.Errorf("failed to read chunk '%s': %w", chunkID, err)
		}

		if chunkInfo.Padding > 0 {
			if len(chunkBytes) > chunkInfo.Padding {
				chunkBytes = chunkBytes[:len(chunkBytes)-chunkInfo.Padding]
			}
		}

		resultedFile = append(resultedFile, chunkBytes...)
	}

	return resultedFile, nil
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
