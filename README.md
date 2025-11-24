# chunkmesh
`chunkMesh` is a lightweight file storage system designed to offer efficient, resilient and versioned archiving through chunk-level data deduplication. This project aims to optimize storage space by reducing data redundancy and provide granular control over file modifications over time.


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



* **Chunk-Level Deduplication**: Files are splitted into fixed-size blocks (chunks). Only unique chunks are physically stored, while duplicate files or file versions that share existing chunks reference the blocks already present. This reduces the required storage space.

* **Automatic Versioning**: Each time a file is added (or modified if already exists), `chunkmesh` automatically creates a new version, maintaining a complete history of changes without duplicating the entire file. Each version is an aggregation of references to chunks.

* **Atomic State Management**: Storage modification operations (additions, deletions) are protected by locking mechanisms (mutexes) to ensure data consistency and integrity in concurrent environments. The metadata state (which files, versions and chunks exist) is persisted in a single JSON file.

* **Intelligent Deletion**: When a file version is deleted, `chunkmesh` decrements the reference counter (`ref_count`) for each of its chunks. Only chunks that are no longer referenced by any version of any file are physically removed from disk, ensuring that valuable data is not prematurely lost.



---



## Technology Stack



* **Core Language**: Go (Golang)

* **Database**: The database is local and file-based: chunkmesh.json for metadata, 'hash chunk'.chunk for chunk content
---



## Installation and Setup


> [!WARNING]
> ### Prerequisites
>
>
>
> * Go (v 1.22 or later)


### 1. How to Use

**Initialization and Configuration**

```
import "github.com/FraMan97/chunkmesh"
storage, err := chunkmesh.NewChunkMeshStorage(
    "/data/my_storage", // path to local folder
    4096, // chunk size
)
if err != nil { /* handle error */ }
```
**Add file and Delete file**

```
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
---



## Roadmap



Here is future planned developments:

* Add file get by version id
* Add optional chunk compression
* Evaluate check integrity of a file by version id


