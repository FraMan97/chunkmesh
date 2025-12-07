package models

import (
	"context"
	"io"

	"github.com/FraMan97/chunkmesh/pkg"
)

type Store interface {
	AddByInfo(context context.Context, fileName string, data []byte, options *pkg.StoreObjectOptions) (string, error)
	AddByPath(context context.Context, fileName string, filePath string, options *pkg.StoreObjectOptions) (string, error)
	Delete(context context.Context, fileName string, versionId string) error
	Get(context context.Context, fileName string, versionIdRequested string, passphrase string, dst io.Writer) error
	CleanUp(context context.Context)
	GetLatestVersion(context context.Context, fileName string) (string, error)
}
