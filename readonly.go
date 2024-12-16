package filesystem

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/gopi-frame/contract/filesystem"
)

var ErrReadOnly = errors.New("read-only file system")

type ReadOnlyFileSystem struct {
	fs filesystem.FileSystem
}

func NewReadOnlyFileSystem(fs filesystem.FileSystem) *ReadOnlyFileSystem {
	return &ReadOnlyFileSystem{
		fs,
	}
}

func (r *ReadOnlyFileSystem) Exists(path string) (bool, error) {
	return r.fs.Exists(path)
}

func (r *ReadOnlyFileSystem) FileExists(path string) (bool, error) {
	return r.fs.FileExists(path)
}

func (r *ReadOnlyFileSystem) DirExists(path string) (bool, error) {
	return r.fs.DirExists(path)
}

func (r *ReadOnlyFileSystem) Read(path string) ([]byte, error) {
	return r.fs.Read(path)
}

func (r *ReadOnlyFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	return r.fs.ReadStream(path)
}

func (r *ReadOnlyFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	return r.fs.ReadDir(path)
}

func (r *ReadOnlyFileSystem) WalkDir(path string, walkFn fs.WalkDirFunc) error {
	return r.fs.WalkDir(path, walkFn)
}

func (r *ReadOnlyFileSystem) LastModified(path string) (time.Time, error) {
	return r.fs.LastModified(path)
}

func (r *ReadOnlyFileSystem) FileSize(path string) (int64, error) {
	return r.fs.FileSize(path)
}

func (r *ReadOnlyFileSystem) MimeType(path string) (string, error) {
	return r.fs.MimeType(path)
}

func (r *ReadOnlyFileSystem) Visibility(path string) (string, error) {
	return r.fs.Visibility(path)
}

func (r *ReadOnlyFileSystem) Write(location string, content []byte, config filesystem.CreateFileConfig) error {
	return NewUnableToWriteFile(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) WriteStream(location string, stream io.Reader, config filesystem.CreateFileConfig) error {
	return NewUnableToWriteFile(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) SetVisibility(location string, visibility string) error {
	return NewUnableToSetPermission(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) Delete(location string) error {
	return NewUnableToDeleteFile(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) DeleteDir(location string) error {
	return NewUnableToDeleteDirectory(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) CreateDir(location string, config filesystem.CreateDirectoryConfig) error {
	return NewUnableToCreateDirectory(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) Move(src string, dst string, config filesystem.CreateDirectoryConfig) error {
	return NewUnableToMove(src, dst, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) Copy(src string, dst string, config filesystem.CreateFileConfig) error {
	return NewUnableToCopyFile(src, dst, ErrReadOnly)
}
