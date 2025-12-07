# chunkmesh

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=for-the-badge&logo=go&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)
![BoltDB](https://img.shields.io/badge/BoltDB-BB3333?style=for-the-badge&logo=google-cloud&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![License](https://img.shields.io/badge/License-GNU_AGPL_v3-blue?style=for-the-badge&logo=gnu&logoColor=white)

`chunkmesh` is a hybrid Content-Addressable Storage (CAS) system designed to offer efficient, resilient, and versioned archiving through chunk-level data deduplication. 
Built to support **Local Filesystem**, **AWS S3**, and **Google Cloud Storage (GCS)**, this project optimizes storage space and network bandwidth by eliminating data redundancy and providing granular control over file lifecycle.

---

## Table of Contents

* [Key Features](#key-features)
* [Technology Stack](#technology-stack)
* [Installation and Setup](#installation-and-setup)
    * [Prerequisites](#prerequisites)
    * [How to Use](#1-how-to-use)
* [Road Map](#road-map)


---

## Key Features

* **Hybrid Backend Support**: Seamlessly store data across Local Filesystems, AWS S3, and Google Cloud Storage while keeping metadata fast and local.
* **Content-Defined Deduplication**: Uses the **FastCDC** algorithm to split files into variable-size chunks. This ensures that minor insertions or shifts only affect local chunks, maximizing deduplication efficiency and minimizing cloud egress/ingress.
* **Data Integrity Assurance**: Automatically performs SHA-256 integrity checks during retrieval to detect silent data corruption.
* **AES Encryption**: Supports optional AES-256 (GCM) client-side encryption. Chunks are encrypted before being written to disk or uploaded to the cloud.
* **Retention Policy**: Supports optional Time-to-Live (TTL). Expired versions are automatically purged during cleanup.
* **Stream-Based Processing**: Built around `io.Reader`, enabling processing of large files with constant, minimal RAM usage.
* **Atomic State Management**: Metadata is managed via a transactional local Key/Value store (BoltDB).

---

## Technology Stack

* **Core Language**: Go (Golang) v1.24+
* **Metadata Store**: Embedded Key/Value store (BoltDB) for file registry, version trees, and chunk reference counting.
* **Blob Storage**:
    * **Local**: Sharded directory structure for efficient file organization (e.g., `chunks/ab/cd/...`).
    * **S3/GCS**: Object storage buckets with content-addressable keys.

---

## Installation and Setup

> [!NOTE]
> ### Prerequisites
> * Go (v 1.24 or later).

### 1. How to Use

**Initialization (Select a Provider)**

You can initialize the storage using one of the available providers (`local`, `s3`, `gcs`).

```go
import (
    "context"
    "github.com/FraMan97/chunkmesh/local"
    "github.com/FraMan97/chunkmesh/s3"
    "github.com/FraMan97/chunkmesh/gcs"
    "github.com/FraMan97/chunkmesh/pkg"
)

func main() {
    ctx := context.Background()
    avgChunkSize := 5000 // Target average chunk size in bytes

    // Option A: Local Storage
    store, _ := local.NewLocalStorage(ctx, &local.LocalStorageOptions{
        MetadataDirectoryPath: "/path/to/local",
        BucketChunksPath: "/database/chunks",
        AvgChunkSize:          avgChunkSize,
    })

    // Option B: AWS S3
    store, _ := s3store.NewS3Storage(ctx, &s3store.S3StorageOptions{
        Bucket:           "my-bucket",
        Region:           "us-east-1",
        MetadataDirectoryPath: "/path/to/local",
        BucketChunksPath: "/database/chunks",
        AvgChunkSize:     avgChunkSize,
     })
    
    // Option C: Google Cloud Storage
    store, _ := gcs.NewGCSStorage(ctx, &gcs.GCSStorageOptions{
        Bucket:           "my-gcs-bucket",
        ProjectId:        "my-project-id",
        MetadataDirectoryPath: "/path/to/local",
        BucketChunksPath: "/database/chunks",
        AvgChunkSize:     avgChunkSize,
     })
}
```

**Add File**


```go
// Configure upload options
opts := &pkg.StoreObjectOptions{
    Compress:   true,           // Enable GZIP compression
    Retention:  3600,           // TTL in seconds (0 = forever)
    Passphrase: "secret-key",   // Encryption passphrase ("" = no encryption)
}

// Add by Path: (Context, File Name, Local File Path, Options)
versionID, err := store.AddByPath(ctx, "report.pdf", "/local/path/to/report.pdf", opts)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("File uploaded successfully. Version ID: %s\n", versionID)

```

**Get File**


```go
// Download File: (File Name)
outputFile, err := os.Create("downloaded_report.pdf")
if err != nil {
    log.Fatal(err)
}
defer outputFile.Close()

// Get File: (Context, File Name, Version ID or "latest", Passphrase or "", Destination)
err = store.Get(
    ctx, 
    "report.pdf", 
    "latest",          
    "secret-key",      
    outputFile,        
)
if err != nil {
    log.Fatal(err)
}

```
**Delete File**


```go
// Delete File Version: (File Name, VersionId or "latest")
err := store.Delete(ctx, "report.pdf", "latest")
if err != nil {
    log.Fatal(err)
}

```
**Maintenance**


```go
// Clean Database: (Context)
store.CleanUp(ctx)

```
**Backup (Cloud only)**


```go
// Backup Metadata File (Context, Backup Bucket, Bucket Path)
err := store.BackupMetadata(ctx, "my-backup-bucket", "backups/metadata.db")
if err != nil {
    log.Printf("Backup failed: %v", err)
}
```


## Road Map

* [] Add MongoDB as metadata database support
* [] Add Postgresql as metadata database support
* [] Add Azure as cloud storage support
