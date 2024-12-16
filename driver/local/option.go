package local

import (
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem/visibility/unix"
)

var noneOption = OptionFunc(func(lfs *LocalFileSystem) error {
	return nil
})

type OptionFunc func(*LocalFileSystem) error

func (f OptionFunc) Apply(lfs *LocalFileSystem) error {
	return f(lfs)
}

func WithDeferRootCreation() OptionFunc {
	return func(lfs *LocalFileSystem) error {
		lfs.deferRootCreation = true
		return nil
	}
}

func WithMimeTypeDetector(detector fs.MimeTypeDetector) OptionFunc {
	if detector == nil {
		return noneOption
	}
	return func(lfs *LocalFileSystem) error {
		lfs.mimetypeDetector = detector
		return nil
	}
}

func WithVisibilityConvertor(convertor unix.VisibilityConvertor) OptionFunc {
	if convertor == nil {
		return noneOption
	}
	return func(lfs *LocalFileSystem) error {
		lfs.visibilityConvertor = convertor
		return nil
	}
}
