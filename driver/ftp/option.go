package ftp

import (
	"github.com/gopi-frame/contract"
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem/visibility/unix"
)

type Option = contract.Option[*FTPFileSystem]

type OptionFunc func(*FTPFileSystem) error

func (f OptionFunc) Apply(fs *FTPFileSystem) error {
	return f(fs)
}

var noneOption = OptionFunc(func(fs *FTPFileSystem) error { return nil })

func WithConnPool(pool ConnPool) Option {
	if pool == nil {
		return noneOption
	}
	return OptionFunc(func(fs *FTPFileSystem) error {
		fs.connPool = pool
		return nil
	})
}

func WithMimeTypeDetector(detector fs.MimeTypeDetector) Option {
	if detector == nil {
		return noneOption
	}
	return OptionFunc(func(fs *FTPFileSystem) error {
		fs.mimetypeDetector = detector
		return nil
	})
}

func WithVisibilityConvertor(convertor unix.VisibilityConvertor) Option {
	if convertor == nil {
		return noneOption
	}
	return OptionFunc(func(fs *FTPFileSystem) error {
		fs.visibilityConvertor = convertor
		return nil
	})
}
