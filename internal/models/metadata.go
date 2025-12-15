package models

import (
	"context"
)

type MetadataStore interface {
	EnsureCollection(context context.Context, collection string) error
	GetData(context context.Context, collection string, key string) ([]byte, error)
	PutData(context context.Context, collection string, key string, data []byte) error
	DeleteKey(context context.Context, collection string, key string) error
	ExistsKey(context context.Context, collection string, key string) (bool, error)
	GetAllData(context context.Context, collection string) (map[string][]byte, error)
	GetAllKeys(context context.Context, collection string) ([]string, error)
	Close(context context.Context) error
}
