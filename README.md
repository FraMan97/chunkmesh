# chunkmesh
`chunkmesh` is a lightweight, local file storage system designed to offer efficient, resilient and versioned archiving through chunk-level data deduplication. This project optimizes storage space by eliminating data redundancy and providing granular control over file modifications over time.

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

* **Chunk-Level Deduplication**: Files are split into fixed-size blocks (chunks). Only unique chunks are physically stored, while duplicate files or versions referencing existing chunks simply point to the stored data.
* **Efficient Compression**: Uses standard GZIP compression (`gzip.DefaultCompression`) to balance speed and storage efficiency.
* **Data Integrity Assurance**: Automatically performs SHA-256 integrity checks during file retrieval (`Get`) to detect and prevent silent data corruption immediately upon access.
* **Retention Policy**: Supports optional Time-to-Live (TTL) for files. Versions older than the specified retention period are automatically purged during cleanup.
* **Stream-Based Processing**: Built on Go's `io.Reader` interfaces, `chunkmesh` processes files of any size (GBs or TBs) using a constant, minimal amount of RAM.
* **Optimized File Layout (Sharding)**: Chunks are stored using a directory sharding strategy (e.g., `chunks/ab/cd/...`) to prevent filesystem performance degradation.
* **Atomic-like State Management**: Operations are protected by locking mechanisms and transactional metadata updates.
* **Intelligent Garbage Collection**: A comprehensive clean-up mechanism removes orphan chunks, expired versions, corrupted versions (through SHA-256 integrity checks of version chunks) and empty directories.

---

## Technology Stack

* **Core Language**: Go (Golang)
* **Database**: Embedded Key/Value store (BoltDB):
    * `boltdb.db`: Stores metadata, file registry, chunk references and version history using efficient, transactional B+ trees.
    * `chunks/`: Stores the deduplicated (and optionally compressed) binary content on the filesystem.

---

## Installation and Setup

> [!NOTE]
> ### Prerequisites
> * Go (v 1.24 or later)

### 1. How to Use

**Initialization**

```go
import "github.com/FraMan97/chunkmesh/src/core"

// Use a larger chunk size (e.g., 4MB) for better performance on large files
const MB = 1024 * 1024

storage, err := core.NewChunkMeshStorage(
    "/data/my_storage", // Path to local folder
    4 * MB,             // Chunk size
)
if err != nil { /* handle error */ }
```

**Add file and Delete file**

```go

// Add file (version) by local path
err := storage.AddByPath(
    "file.txt", // File name. Include the extension for logical grouping
    "path/to/file.txt", // Full local path to the file.
    true, // Enable default compression with gzip
    3600, // Retention Policy in seconds. '0' means no future deletion
)
if err != nil { /* handle error */ }

// Add file (version) by []byte content
err = storage.AddByInfo(
    "file.txt", // File name. Include the extension for logical grouping
    content, // File content in []byte
    true, // Enable default compression with gzip
    3600, // Retention Policy in seconds. '0' means no future deletion
)
if err != nil { /* handle error */ }

// Delete file (version)
err = storage.Delete(
    "file.txt", // File name. Include the extension for logical grouping
    "anshd...", // Version id to delete or use "latest" for the most recent version
)
if err != nil { /* handle error */ }
```

**Get file**

```go

// Get file (version)
content, err := storage.Get(
    "file.txt", //File name. Include the extension for logical grouping
    "anshd...", //Version id to get or use "latest" for the most recent version
)
if err != nil { /* handle error */ }
```

**Clean database**

```go

// Clean orphan chunks, corrupted versions, expired versions and empty directories
storage.CleanUp()
```

**Get latest file version**

```go

latest, err := storage.GetLatestVersion(
    "file.txt", //File name. Include the extension for logical grouping
)
if err != nil { /* handle error */ }
```
---



## Roadmap

* Add AES Encryption for chunks
