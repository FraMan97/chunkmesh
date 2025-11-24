# chunkmesh
`chunkMesh` is a lightweight, local file storage system designed to offer efficient, resilient, and versioned archiving through chunk-level data deduplication. This project optimizes storage space by eliminating data redundancy and providing granular control over file modifications over time.

> [!WARNING]
> This project is currently a Proof of Concept (POC) and is not suitable for a production environment. It is intended for developer use only.

---

## Table of Contents

* [Key Features](#key-features)
* [Technology Stack](#technology-stack)
* [Installation and Setup](#installation-and-setup)
    * [Prerequisites](#prerequisites)
    * [How to Use](#1-how-to-use)
* [Roadmap](#roadmap)

---

## Key Features

* **Chunk-Level Deduplication**: Files are split into fixed-size blocks (chunks). Only unique chunks are physically stored, while duplicate files or versions referencing existing chunks simply point to the stored data. This drastically reduces storage requirements.
* **Stream-Based Processing (Low Memory Footprint)**: Built on Go's `io.Reader` interfaces, `chunkmesh` processes files of any size (GBs or TBs) using a constant, minimal amount of RAM.
* **Optimized File Layout (Sharding)**: Chunks are stored using a directory sharding strategy (e.g., `chunks/ab/cd/...`) to prevent filesystem performance degradation when storing millions of chunks.
* **Automatic Versioning**: Every addition or modification creates a new version, maintaining a complete history of changes without duplicating unchanged parts of the file.
* **Atomic State Management**: Operations are protected by locking mechanisms (mutexes) to ensure data consistency. Metadata is persisted in a synchronized JSON registry.
* **Intelligent Deletion**: A reference counting system (`ref_count`) ensures that chunks are physically deleted from the disk only when no active file version references them anymore.

---

## Technology Stack

* **Core Language**: Go (Golang)
* **Database**: Local file-based system:
    * `chunkmesh.json`: Stores metadata, file registry, and version history.
    * `chunks/`: Stores the deduplicated binary content using SHA-256 sharded paths.

---

## Installation and Setup

> [!NOTE]
> ### Prerequisites
> * Go (v 1.22 or later)

### 1. How to Use

**Initialization**

```go
import "[github.com/FraMan97/chunkmesh](https://github.com/FraMan97/chunkmesh)"

storage, err := chunkmesh.NewChunkMeshStorage(
    "/data/my_storage", // Path to local folder
    4096,               // Chunk size (e.g., 4KB)
)
if err != nil { /* handle error */ }
```

**Add file and Delete file**

```go

// add file (version) by local path
err := storage.AddByPath(
    "file.txt", //File name. Include the extension for logical grouping
    "path/to/file.txt", // Full local path to the file.
)
if err != nil { /* handle error */ }

// add file (version) by []byte content
err := storage.AddByInfo(
    "file.txt", //file name. Include the extension
    content, //file content in []byte
)
if err != nil { /* handle error */ }

// delete file (version)
err := storage.Delete(
    "file.txt", //File name. Include the extension for logical grouping
    "anshd...", //Version id to delete or use "latest" for the most recent version
)
if err != nil { /* handle error */ }
```

**Get file**

```go

// get file (version)
content, err := storage.Get(
    "file.txt", //File name. Include the extension for logical grouping
    "anshd...", //Version id to get or use "latest" for the most recent version
)
if err != nil { /* handle error */ }
```
---



## Roadmap



Here is future planned developments:

* Add optional chunk compression
* Evaluate check integrity of a file by version id


