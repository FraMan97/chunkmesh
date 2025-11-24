package chunkmesh

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"path/filepath"
	"time"
)

func generateHash(data []byte) string {
	sha := sha256.New()
	sha.Write(data)
	hash := sha.Sum(nil)
	return hex.EncodeToString(hash)
}

func processChunks(r io.Reader, chunkSize int, compression bool, handler func(chunk []byte) error) (int, error) {
	buf := make([]byte, chunkSize)
	totalSize := 0

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			totalSize += n
			chunkData := buf[:n]
			if compression {
				chunkData, err = compress(chunkData)
				if err != nil {
					return totalSize, err
				}
			}
			if err := handler(chunkData); err != nil {
				return totalSize, err
			}
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return totalSize, err
		}
	}
	return totalSize, nil
}

func listFiles(dir string) []string {
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

func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
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

func decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
