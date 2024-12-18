package filesystem

import (
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/gopi-frame/contract/filesystem"
)

// DeferFileSystem is a wrapper around a FileSystem
// that defers the creation of the file system instance until the first operation is performed.
type DeferFileSystem struct {
	fs filesystem.FileSystem

	once    sync.Once
	driver  string
	options map[string]any
}

func NewDeferFileSystem(driver string, options map[string]any) *DeferFileSystem {
	return &DeferFileSystem{driver: driver, options: options}
}

func (fs *DeferFileSystem) deferInit() error {
	var err error
	fs.once.Do(func() {
		fs.fs, err = Open(fs.driver, fs.options)
	})
	return err
}

func (fs *DeferFileSystem) Exists(path string) (bool, error) {
	if err := fs.deferInit(); err != nil {
		return false, err
	}
	return fs.fs.Exists(path)
}

func (fs *DeferFileSystem) FileExists(path string) (bool, error) {
	if err := fs.deferInit(); err != nil {
		return false, err
	}
	return fs.fs.FileExists(path)
}

func (fs *DeferFileSystem) DirExists(path string) (bool, error) {
	if err := fs.deferInit(); err != nil {
		return false, err
	}
	return fs.fs.DirExists(path)
}

func (fs *DeferFileSystem) Read(path string) ([]byte, error) {
	if err := fs.deferInit(); err != nil {
		return nil, err
	}
	return fs.fs.Read(path)
}

func (fs *DeferFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	if err := fs.deferInit(); err != nil {
		return nil, err
	}
	return fs.fs.ReadStream(path)
}

func (fs *DeferFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	if err := fs.deferInit(); err != nil {
		return nil, err
	}
	return fs.fs.ReadDir(path)
}

func (fs *DeferFileSystem) WalkDir(path string, walkFn fs.WalkDirFunc) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.WalkDir(path, walkFn)
}

func (fs *DeferFileSystem) LastModified(path string) (time.Time, error) {
	if err := fs.deferInit(); err != nil {
		return time.Time{}, err
	}
	return fs.fs.LastModified(path)
}

func (fs *DeferFileSystem) FileSize(path string) (int64, error) {
	if err := fs.deferInit(); err != nil {
		return 0, err
	}
	return fs.fs.FileSize(path)
}

func (fs *DeferFileSystem) MimeType(path string) (string, error) {
	if err := fs.deferInit(); err != nil {
		return "", err
	}
	return fs.fs.MimeType(path)
}

func (fs *DeferFileSystem) Visibility(path string) (string, error) {
	if err := fs.deferInit(); err != nil {
		return "", err
	}
	return fs.fs.Visibility(path)
}

func (fs *DeferFileSystem) Write(path string, content []byte, config map[string]any) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.Write(path, content, config)
}

func (fs *DeferFileSystem) WriteStream(path string, stream io.Reader, config map[string]any) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.WriteStream(path, stream, config)
}

func (fs *DeferFileSystem) SetVisibility(path string, visibility string) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.SetVisibility(path, visibility)
}

func (fs *DeferFileSystem) Delete(path string) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.Delete(path)
}

func (fs *DeferFileSystem) DeleteDir(path string) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.DeleteDir(path)
}

func (fs *DeferFileSystem) CreateDir(path string, config map[string]any) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.CreateDir(path, config)
}

func (fs *DeferFileSystem) Move(src string, dst string, config map[string]any) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.Move(src, dst, config)
}

func (fs *DeferFileSystem) Copy(src string, dst string, config map[string]any) error {
	if err := fs.deferInit(); err != nil {
		return err
	}
	return fs.fs.Copy(src, dst, config)
}
