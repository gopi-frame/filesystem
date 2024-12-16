package minio

import (
	"github.com/gopi-frame/contract"
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/minio/minio-go/v7"
)

type Option = contract.Option[*MinioFileSystem]

type OptionFunc func(fs *MinioFileSystem) error

func (f OptionFunc) Apply(fs *MinioFileSystem) error {
	return f(fs)
}

var noneOption = OptionFunc(func(fs *MinioFileSystem) error {
	return nil
})

func WithClient(client *minio.Client) Option {
	if client == nil {
		return noneOption
	}
	return OptionFunc(func(fs *MinioFileSystem) error {
		fs.client = client
		return nil
	})
}

func WithBucket(bucket string) Option {
	return OptionFunc(func(fs *MinioFileSystem) error {
		fs.bucket = bucket
		return nil
	})
}

func WithMimeTypeDetector(detector fs.MimeTypeDetector) Option {
	if detector == nil {
		return noneOption
	}
	return OptionFunc(func(fs *MinioFileSystem) error {
		fs.mimeTypeDetector = detector
		return nil
	})
}
