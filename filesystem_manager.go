package filesystem

import (
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gopi-frame/contract/filesystem"
)

// FileSystemManager is a manager for filesystem.FileSystem instances.
type FileSystemManager struct {
	mu          *sync.RWMutex
	filesystems map[string]filesystem.FileSystem
}

// NewFileSystemManager creates a new instance of FileSystemManager.
func NewFileSystemManager() *FileSystemManager {
	return &FileSystemManager{
		mu:          &sync.RWMutex{},
		filesystems: make(map[string]filesystem.FileSystem),
	}
}

// AddFS adds a filesystem to the manager.
func (fm *FileSystemManager) AddFS(name string, fs filesystem.FileSystem) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.filesystems[name] = fs
}

// TryGetFS returns a filesystem by name.
// It will return an error if the filesystem is not configured or error occurs.
func (fm *FileSystemManager) TryGetFS(name string) (filesystem.FileSystem, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	f, ok := fm.filesystems[name]
	if !ok {
		return nil, NewUnknownFileSystemError(name)
	}
	return f, nil
}

// GetFS returns a filesystem by name.
// It will panic if the filesystem is not configured or error occurs.
func (fm *FileSystemManager) GetFS(name string) filesystem.FileSystem {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	f, ok := fm.filesystems[name]
	if !ok {
		panic(NewUnknownFileSystemError(name))
	}
	return f
}

// HasFS checks if the filesystem exists.
func (fm *FileSystemManager) HasFS(name string) bool {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	_, ok := fm.filesystems[name]
	return ok
}

func (fm *FileSystemManager) splitFileSystemAndPath(path string) (filesystem.FileSystem, string, error) {
	parts := strings.Split(path, "://")
	if len(parts) != 2 {
		return nil, "", NewInvalidPathError(path)
	}
	fsName := parts[0]
	f, err := fm.TryGetFS(fsName)
	if err != nil {
		return nil, "", err
	}
	return f, parts[1], nil
}

// Exists checks if the file or directory exists.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file or directory.
func (fm *FileSystemManager) Exists(p string) (bool, error) {
	f, p, err := fm.splitFileSystemAndPath(p)
	if err != nil {
		return false, err
	}
	return f.Exists(p)
}

// FileExists checks if the file exists.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) FileExists(path string) (bool, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return false, err
	}
	return f.FileExists(p)
}

// DirExists checks if the directory exists.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the directory.
func (fm *FileSystemManager) DirExists(path string) (bool, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return false, err
	}
	return f.DirExists(p)
}

// Read reads the file content.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) Read(path string) ([]byte, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return nil, err
	}
	return f.Read(p)
}

// ReadStream reads the file content as a stream.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) ReadStream(path string) (io.ReadCloser, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return nil, err
	}
	return f.ReadStream(p)
}

// ReadDir reads the directory content.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the directory.
func (fm *FileSystemManager) ReadDir(path string) ([]os.DirEntry, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return nil, err
	}
	return f.ReadDir(p)
}

// WalkDir walks the directory tree.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the directory.
func (fm *FileSystemManager) WalkDir(path string, walkFn fs.WalkDirFunc) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return err
	}
	return f.WalkDir(p, walkFn)
}

// LastModified returns the last modified time of the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) LastModified(path string) (time.Time, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return time.Time{}, err
	}
	return f.LastModified(p)
}

// FileSize returns the size of the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) FileSize(path string) (int64, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return 0, err
	}
	return f.FileSize(p)
}

// MimeType returns the mime type of the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) MimeType(path string) (string, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return "", err
	}
	return f.MimeType(p)
}

// Visibility returns the visibility of the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) Visibility(path string) (string, error) {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return "", err
	}
	return f.Visibility(p)
}

// Write writes the content to the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) Write(path string, content []byte, config map[string]any) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return err
	}
	return f.Write(p, content, config)
}

// WriteStream writes the content to the file as a stream.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) WriteStream(path string, stream io.Reader, config map[string]any) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return nil
	}
	return f.WriteStream(p, stream, config)
}

// SetVisibility sets the visibility of the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) SetVisibility(path string, visibility string) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return err
	}
	return f.SetVisibility(p, visibility)
}

// Delete deletes the file.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the file.
func (fm *FileSystemManager) Delete(path string) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return err
	}
	return f.Delete(p)
}

// DeleteDir deletes the directory.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the directory.
func (fm *FileSystemManager) DeleteDir(path string) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return err
	}
	return f.DeleteDir(p)
}

// CreateDir creates the directory.
//
// Path should be in the format of "<fs>://<path>",
// where <fs> is the name of the filesystem and <path> is the path to the directory.
func (fm *FileSystemManager) CreateDir(path string, config map[string]any) error {
	f, p, err := fm.splitFileSystemAndPath(path)
	if err != nil {
		return err
	}
	return f.CreateDir(p, config)
}

// Move moves the file or directory (if supported) to the new location.
//
// Source path and destination path should be in the format of "<fs>://<path>"
// where <fs> is the name of the filesystem and <path> is the path to the file or directory.
//
// Note:
//
//	When the source filesystem and destination filesystem is not the same, moving directory is not supported.
func (fm *FileSystemManager) Move(src string, dst string, config map[string]any) error {
	f1, p1, err := fm.splitFileSystemAndPath(src)
	if err != nil {
		return err
	}
	f2, p2, err := fm.splitFileSystemAndPath(dst)
	if err != nil {
		return err
	}
	if f1 == f2 {
		return f1.Move(p1, p2, config)
	}
	s1, err := f1.ReadStream(p1)
	if err != nil {
		return err
	}
	defer s1.Close()
	return f2.WriteStream(p2, s1, nil)
}

// Copy copies the source file to the destination location
//
// Source path and destination path should be in the format of "<fs>://<path>"
// where <fs> is the name of the filesystem and <path> is the path to the file or directory.
func (fm *FileSystemManager) Copy(src string, dst string, config map[string]any) error {
	f1, p1, err := fm.splitFileSystemAndPath(src)
	if err != nil {
		return err
	}
	f2, p2, err := fm.splitFileSystemAndPath(dst)
	if err != nil {
		return err
	}
	if f1 == f2 {
		return f1.Copy(p1, p2, config)
	}
	s1, err := f1.ReadStream(p1)
	if err != nil {
		return err
	}
	defer s1.Close()
	return f2.WriteStream(p2, s1, nil)
}
