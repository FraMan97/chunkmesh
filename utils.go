package chunkmesh

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

func generateHash(data []byte) string {
	sha := sha256.New()
	sha.Write(data)
	hash := sha.Sum(nil)
	return hex.EncodeToString(hash)
}

func ProcessChunks(r io.Reader, chunkSize int, handler func(chunk []byte, padding int) error) (int, error) {
	buf := make([]byte, chunkSize)
	totalSize := 0

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			totalSize += n
			currentPadding := 0

			chunkData := buf[:n]
			if n < chunkSize {
				currentPadding = chunkSize - n
				paddedChunk := make([]byte, chunkSize)
				copy(paddedChunk, chunkData)
				chunkData = paddedChunk
			}

			if err := handler(chunkData, currentPadding); err != nil {
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
