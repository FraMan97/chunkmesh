package utils

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	"github.com/jotfs/fastcdc-go"
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
