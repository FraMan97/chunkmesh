package boltdb_manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

type BoltDBMetadataStorage struct {
	db *bolt.DB
}

func OpenDatabase(path string) (*BoltDBMetadataStorage, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bolt database folder '%s'", path)
	}
	db, err := bolt.Open(filepath.Join(path, "boltdb.db"), 0600, &bolt.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open the bolt database in the folder '%s'", path)
	}

	return &BoltDBMetadataStorage{
		db: db,
	}, nil
}

func (b *BoltDBMetadataStorage) EnsureCollection(context context.Context, bucketName string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("failed to create if not exists the bucket '%s'", bucketName)
		}
		return nil
	})
}

func (b *BoltDBMetadataStorage) GetData(context context.Context, bucketName string, key string) ([]byte, error) {
	var value []byte

	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket '%s' not found", bucketName)
		}

		v := b.Get([]byte(key))
		if v == nil {
			return fmt.Errorf("key '%s' not found in bucket '%s'", key, bucketName)
		}

		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (b *BoltDBMetadataStorage) PutData(context context.Context, bucketName string, key string, data []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket '%s' not found", bucketName)
		}
		err := b.Put([]byte(key), data)
		if err != nil {
			return err
		}
		return nil
	})
}

func (b *BoltDBMetadataStorage) DeleteKey(context context.Context, bucketName string, key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket '%s' not found", bucketName)
		}
		err := b.Delete([]byte(key))
		if err != nil {
			return err
		}
		return nil
	})
}

func (b *BoltDBMetadataStorage) ExistsKey(context context.Context, bucketName string, key string) (bool, error) {
	found := false

	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket '%s' not found", bucketName)
		}

		v := b.Get([]byte(key))
		if v != nil {
			found = true
		}

		return nil
	})

	return found, err
}

func (b *BoltDBMetadataStorage) GetAllData(context context.Context, bucketName string) (map[string][]byte, error) {
	values := make(map[string][]byte)

	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket '%s' not found in DB", bucketName)
		}

		return b.ForEach(func(k []byte, v []byte) error {
			valueCopy := make([]byte, len(v))
			copy(valueCopy, v)

			values[string(k)] = valueCopy
			return nil
		})

	})

	if err != nil {
		return nil, err
	}

	return values, nil
}

func (b *BoltDBMetadataStorage) GetAllKeys(context context.Context, bucketName string) ([]string, error) {
	values := []string{}

	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket '%s' not found", bucketName)
		}

		return b.ForEach(func(k []byte, v []byte) error {
			values = append(values, string(k))
			return nil
		})

	})

	if err != nil {
		return nil, err
	}

	return values, nil
}

func (b *BoltDBMetadataStorage) Close(context context.Context) error {
	return b.db.Close()
}
