package readonly

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/gopi-frame/filesystem"

	fs2 "github.com/gopi-frame/contract/filesystem"
)

var ErrReadOnly = errors.New("read-only file system")

type ReadOnlyFileSystem struct {
	f fs2.FileSystem
}

func NewReadOnlyFileSystem(f fs2.FileSystem) *ReadOnlyFileSystem {
	return &ReadOnlyFileSystem{
		f,
	}
}

func (r *ReadOnlyFileSystem) Exists(path string) (bool, error) {
	return r.f.Exists(path)
}

func (r *ReadOnlyFileSystem) FileExists(path string) (bool, error) {
	return r.f.FileExists(path)
}

func (r *ReadOnlyFileSystem) DirExists(path string) (bool, error) {
	return r.f.DirExists(path)
}

func (r *ReadOnlyFileSystem) Read(path string) ([]byte, error) {
	return r.f.Read(path)
}

func (r *ReadOnlyFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	return r.f.ReadStream(path)
}

func (r *ReadOnlyFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	return r.f.ReadDir(path)
}

func (r *ReadOnlyFileSystem) WalkDir(path string, walkFn fs.WalkDirFunc) error {
	return r.f.WalkDir(path, walkFn)
}

func (r *ReadOnlyFileSystem) LastModified(path string) (time.Time, error) {
	return r.f.LastModified(path)
}

func (r *ReadOnlyFileSystem) FileSize(path string) (int64, error) {
	return r.f.FileSize(path)
}

func (r *ReadOnlyFileSystem) MimeType(path string) (string, error) {
	return r.f.MimeType(path)
}

func (r *ReadOnlyFileSystem) Visibility(path string) (string, error) {
	return r.f.Visibility(path)
}

func (r *ReadOnlyFileSystem) Write(location string, content []byte, config map[string]any) error {
	return filesystem.NewUnableToWriteFile(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) WriteStream(location string, stream io.Reader, config map[string]any) error {
	return filesystem.NewUnableToWriteFile(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) SetVisibility(location string, visibility string) error {
	return filesystem.NewUnableToSetPermission(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) Delete(location string) error {
	return filesystem.NewUnableToDeleteFile(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) DeleteDir(location string) error {
	return filesystem.NewUnableToDeleteDirectory(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) CreateDir(location string, config map[string]any) error {
	return filesystem.NewUnableToCreateDirectory(location, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) Move(src string, dst string, config map[string]any) error {
	return filesystem.NewUnableToMove(src, dst, ErrReadOnly)
}

func (r *ReadOnlyFileSystem) Copy(src string, dst string, config map[string]any) error {
	return filesystem.NewUnableToCopyFile(src, dst, ErrReadOnly)
}
