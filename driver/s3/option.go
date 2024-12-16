package s3

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gopi-frame/contract"
	"github.com/gopi-frame/contract/filesystem"
)

type Option = contract.Option[*S3FileSystem]

type OptionFunc func(*S3FileSystem) error

func (f OptionFunc) Apply(fs *S3FileSystem) error {
	return f(fs)
}

var noneOption = OptionFunc(func(fs *S3FileSystem) error {
	return nil
})

// WithClient sets the s3 client
func WithClient(client *s3.Client) Option {
	if client == nil {
		return noneOption
	}
	return OptionFunc(func(fs *S3FileSystem) error {
		fs.client = client
		return nil
	})
}

// WithMimeTypeDetector sets the mime type detector
func WithMimeTypeDetector(d filesystem.MimeTypeDetector) Option {
	if d == nil {
		return noneOption
	}
	return OptionFunc(func(fs *S3FileSystem) error {
		fs.mimeTypeDetector = d
		return nil
	})
}
