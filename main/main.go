package main

import (
	"fmt"

	"github.com/FraMan97/chunkmesh/src/core"
)

func main() {
	storage, _ := core.NewChunkMeshStorage("/home/francesco-mancuso/Documents/database", 1024*1024*4)
	storage.AddByPath("foto.png", "/home/francesco-mancuso/Downloads/Gemini_Generated_Image_31bzi31bzi31bzi3.png", false)
	version, _ := storage.GetLatestVersion("foto.png")
	fmt.Println(version)
	//storage.AddByPath("download2.zip", "/home/francesco-mancuso/Downloads/Unconfirmed 481525.crdownload")
}
