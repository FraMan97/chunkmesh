package utils

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/FraMan97/chunkmesh/internal/database"
	"github.com/FraMan97/chunkmesh/internal/models"
	"github.com/FraMan97/chunkmesh/pkg"
	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	"github.com/jotfs/fastcdc-go"
)

const (
	BucketFiles    = "files"
	BucketVersions = "versions"
	BucketChunks   = "chunks"
)

func GenerateHash(data []byte) string {
	sha := sha256.New()
	sha.Write(data)
	hash := sha.Sum(nil)
	return hex.EncodeToString(hash)
}

func ProcessChunks(r io.Reader, avgChunkSize int, compression bool, maxConcurrency int, handler func(chunkIndex int, chunk []byte) error) (int, error) {
	var wg sync.WaitGroup
	totalSize := 0

	errChan := make(chan error, 1)
	sem := make(chan struct{}, maxConcurrency)

	opts := fastcdc.Options{
		AverageSize: avgChunkSize,
		MinSize:     avgChunkSize / 4,
		MaxSize:     avgChunkSize * 4,
	}

	chunker, err := fastcdc.NewChunker(r, opts)
	if err != nil {
		return 0, err
	}

	chunkIndex := 0

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalSize, err
		}

		chunkData := make([]byte, len(chunk.Data))
		copy(chunkData, chunk.Data)

		totalSize += len(chunkData)

		select {
		case err := <-errChan:
			return totalSize, err
		default:
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(idx int, data []byte) {
			defer func() {
				<-sem
				wg.Done()
			}()

			if len(errChan) > 0 {
				return
			}

			var e error
			if compression {
				data, e = Compress(data)
			}

			if e == nil {
				e = handler(idx, data)
			}

			if e != nil {
				select {
				case errChan <- e:
				default:
				}
			}
		}(chunkIndex, chunkData)

		chunkIndex++
	}

	wg.Wait()
	close(errChan)
	close(sem)

	if err := <-errChan; err != nil {
		return totalSize, err
	}

	return totalSize, nil
}

func ListFiles(dir string) []string {
	var files []string

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() && filepath.Ext(path) == ".chunk" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil
	}

	return files
}

func Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	if err != nil {
		return nil, err
	}
	w.ModTime = time.Time{}
	w.Name = ""
	w.Comment = ""
	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func Encrypt(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	hash := sha256.Sum256(data)
	nonce := hash[:gcm.NonceSize()]

	return gcm.Seal(nonce, nonce, data, nil), nil
}

func Decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}

	nonce, encryptedData := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]

	return gcm.Open(nil, nonce, encryptedData, nil)
}

func DeriveKey(passphrase string) []byte {
	hash := sha256.Sum256([]byte(passphrase))
	return hash[:]
}

func CoreAdd(db *bolt.DB, name string, averageChunkSize int,
	options *pkg.StoreObjectOptions, reader io.Reader, saveChunkFn func(chunkId string, data []byte) error) (string, error) {
	var currentFile models.File
	fileData, err := database.GetData(db, BucketFiles, name)

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
		Retention:   options.Retention,
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

	var aesKey []byte
	if options.Passphrase != "" {
		aesKey = DeriveKey(options.Passphrase)
	}

	totalRead, err := ProcessChunks(reader, averageChunkSize, options.Compress, runtime.NumCPU(), func(index int, c []byte) error {
		var processedData []byte
		var err error

		if options.Passphrase != "" {
			processedData, err = Encrypt(c, aesKey)
			if err != nil {
				return err
			}
		} else {
			processedData = c
		}
		chunkId := GenerateHash(processedData)

		mu.Lock()
		chunkData, err := database.GetData(db, BucketChunks, chunkId)
		exists := (err == nil && chunkData != nil)
		mu.Unlock()

		if !exists {
			if err = saveChunkFn(chunkId, processedData); err != nil {
				return err
			}
		}

		mu.Lock()
		defer mu.Unlock()

		chunkData, err = database.GetData(db, BucketChunks, chunkId)
		exists = (err == nil && chunkData != nil)

		var currentChunk models.Chunk

		if !exists {
			currentChunk = models.Chunk{
				Id:          chunkId,
				Compression: options.Compress,
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
		if err := database.PutData(db, BucketChunks, chunkId, chunkBytes); err != nil {
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
	if err := database.PutData(db, BucketVersions, versionId, versionBytes); err != nil {
		return "", err
	}

	fileMetaBytes, err := json.Marshal(currentFile)
	if err != nil {
		return "", err
	}
	if err := database.PutData(db, BucketFiles, name, fileMetaBytes); err != nil {
		return "", err
	}

	return versionId, nil
}

func CoreGet(db *bolt.DB, lock *sync.RWMutex, fileName string, versionIdRequested string, key string, getFromStorage func(chunkId string) ([]byte, error), dst io.Writer) error {
	lock.RLock()
	defer lock.RUnlock()

	fileData, err := database.GetData(db, BucketFiles, fileName)
	if err != nil || fileData == nil {
		return fmt.Errorf("file '%s' not found", fileName)
	}
	var targetFile models.File
	if err := json.Unmarshal(fileData, &targetFile); err != nil {
		return fmt.Errorf("metadata corruption for file '%s': %v", fileName, err)
	}

	var finalVersionID string
	if versionIdRequested == "latest" {
		finalVersionID = targetFile.LastVersion
	} else {
		finalVersionID = versionIdRequested
	}

	versionData, err := database.GetData(db, BucketVersions, finalVersionID)
	if err != nil || versionData == nil {
		return fmt.Errorf("version '%s' not found", finalVersionID)
	}
	var targetVersion models.Version
	if err := json.Unmarshal(versionData, &targetVersion); err != nil {
		return fmt.Errorf("metadata corruption for version '%s': %v", finalVersionID, err)
	}

	for i, chunkID := range targetVersion.Chunks {
		chunkMetaBytes, err := database.GetData(db, BucketChunks, chunkID)
		if err != nil || chunkMetaBytes == nil {
			return fmt.Errorf("chunk '%s' (index %d) not found in DB", chunkID, i)
		}
		var chunkMeta models.Chunk
		if err := json.Unmarshal(chunkMetaBytes, &chunkMeta); err != nil {
			return fmt.Errorf("metadata corruption for chunk '%s': %v", chunkID, err)
		}

		chunkBytes, err := getFromStorage(chunkID)
		if err != nil {
			return err
		}

		if chunkID != GenerateHash(chunkBytes) {
			return fmt.Errorf("integrity check failed for chunk '%s'", chunkID)
		}

		if key != "" {
			aesKey := DeriveKey(key)
			chunkBytes, err = Decrypt(chunkBytes, aesKey)
			if err != nil {
				return fmt.Errorf("decryption failed for chunk '%s': %v", chunkID, err)
			}
		}

		if chunkMeta.Compression {
			chunkBytes, err = Decompress(chunkBytes)
			if err != nil {
				return fmt.Errorf("decompression failed for chunk '%s': %v", chunkID, err)
			}
		}

		if _, err := dst.Write(chunkBytes); err != nil {
			return fmt.Errorf("failed to write chunk '%s' to destination: %v", chunkID, err)
		}
	}

	return nil
}

func CoreDelete(db *bolt.DB, fileName string, versionId string, deleteChunkFn func(chunkId string) error) error {
	fileData, err := database.GetData(db, BucketFiles, fileName)
	if err != nil {
		return fmt.Errorf("file '%s' not found", fileName)
	}
	var targetFile models.File
	json.Unmarshal(fileData, &targetFile)

	idVersion := versionId
	if versionId == "latest" {
		idVersion = targetFile.LastVersion
	}

	vData, err := database.GetData(db, BucketVersions, idVersion)
	if err != nil {
		return fmt.Errorf("version '%s' not found", idVersion)
	}
	var targetVersion models.Version
	json.Unmarshal(vData, &targetVersion)

	for _, chunkID := range targetVersion.Chunks {
		cData, err := database.GetData(db, BucketChunks, chunkID)
		if err != nil {
			continue
		}
		var chunkMeta models.Chunk
		json.Unmarshal(cData, &chunkMeta)

		chunkMeta.RefCount--

		if chunkMeta.RefCount <= 0 {
			if err = deleteChunkFn(chunkMeta.Id); err != nil {
				return err
			}
			database.DeleteKey(db, BucketChunks, chunkID)
		} else {
			updatedCData, _ := json.Marshal(chunkMeta)
			database.PutData(db, BucketChunks, chunkID, updatedCData)
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
		database.DeleteKey(db, BucketFiles, fileName)
	} else {
		updatedFData, _ := json.Marshal(targetFile)
		database.PutData(db, BucketFiles, fileName, updatedFData)
	}

	database.DeleteKey(db, BucketVersions, idVersion)

	return nil
}

func CoreCleanUp(
	db *bolt.DB,
	listPhysicalChunksFn func() ([]string, error),
	readChunkFn func(chunkId string) ([]byte, error),
	deleteChunkFn func(chunkId string) error,
) error {

	physicalChunkIds, err := listPhysicalChunksFn()
	if err == nil {
		for _, chunkID := range physicalChunkIds {

			exists, _ := database.ExistsKey(db, BucketChunks, chunkID)
			if !exists {
				_ = deleteChunkFn(chunkID)
			} else {
				cData, err := database.GetData(db, BucketChunks, chunkID)
				if err == nil {
					var c models.Chunk
					if err := json.Unmarshal(cData, &c); err == nil {
						if c.RefCount <= 0 {
							if err := deleteChunkFn(chunkID); err == nil {
								_ = database.DeleteKey(db, BucketChunks, chunkID)
							}
						}
					}
				}
			}
		}
	}

	vData, err := database.GetAllData(db, BucketVersions)
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	for _, dataBytes := range vData {
		var v models.Version
		if err := json.Unmarshal(dataBytes, &v); err != nil {
			continue
		}

		shouldDelete := false

		if v.Retention > 0 {
			expirationTime := v.CreatedAt.Add(time.Duration(v.Retention) * time.Second)
			if expirationTime.Before(now) {
				shouldDelete = true
			}
		}

		if !shouldDelete {
			for _, chunkID := range v.Chunks {
				chunkData, err := readChunkFn(chunkID)
				if err != nil {
					shouldDelete = true
					break
				}

				hash := GenerateHash(chunkData)
				if hash != chunkID {
					shouldDelete = true
					break
				}
			}
		}
		if shouldDelete {
			_ = CoreDelete(db, v.FileName, v.Id, deleteChunkFn)
		}
	}

	return nil
}
