package chunkmesh

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
)

func generateHash(data []byte) string {
	sha := sha256.New()
	sha.Write(data)
	hash := sha.Sum(nil)
	return hex.EncodeToString(hash)
}

func splitFileIntoChunks(file []byte, sizeChunk int) ([][]byte, []int, error) {
	fileSize := len(file)
	if fileSize == 0 {
		return [][]byte{}, []int{}, nil
	}

	numChunks := int(math.Ceil(float64(fileSize) / float64(sizeChunk)))

	chunks := make([][]byte, 0, numChunks)
	paddings := make([]int, 0, numChunks)

	for i := 0; i < numChunks; i++ {
		start := i * sizeChunk
		end := start + sizeChunk

		if end > fileSize {
			end = fileSize
		}

		currentChunk := file[start:end]
		currentPadding := 0

		if i == numChunks-1 && len(currentChunk) < sizeChunk {
			currentPadding = sizeChunk - len(currentChunk)
			paddingBytes := bytes.Repeat([]byte{0x00}, currentPadding)
			currentChunk = append(currentChunk, paddingBytes...)
		}

		chunks = append(chunks, currentChunk)
		paddings = append(paddings, currentPadding)
	}

	return chunks, paddings, nil
}
