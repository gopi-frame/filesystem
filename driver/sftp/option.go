package sftp

import (
	"github.com/gopi-frame/contract"
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem/visibility/unix"
)

type Option = contract.Option[*SFTPFileSystem]

type OptionFunc func(fs *SFTPFileSystem) error

func (f OptionFunc) Apply(fs *SFTPFileSystem) error {
	return f(fs)
}

var noneOption = OptionFunc(func(fs *SFTPFileSystem) error {
	return nil
})

func WithClientPool(pool ClientPool) OptionFunc {
	if pool == nil {
		return noneOption
	}
	return func(fs *SFTPFileSystem) error {
		fs.clientPool = pool
		return nil
	}
}

func WithMimeTypeDetector(detector fs.MimeTypeDetector) OptionFunc {
	if detector == nil {
		return noneOption
	}
	return func(fs *SFTPFileSystem) error {
		fs.mimeTypeDetector = detector
		return nil
	}
}

func WithVisibilityConvertor(convertor unix.VisibilityConvertor) OptionFunc {
	if convertor == nil {
		return noneOption
	}
	return func(fs *SFTPFileSystem) error {
		fs.visibilityConvertor = convertor
		return nil
	}
}
