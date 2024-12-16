package memory

import (
	"bytes"
	"errors"
	"io"
	gofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

type MemoryFileSystem struct {
	mu               *sync.RWMutex
	root             *dirEntry
	visibility       string
	mimetypeDetector fs.MimeTypeDetector
}

func NewMemoryFileSystem(visibility string, mimetypeDetector fs.MimeTypeDetector) *MemoryFileSystem {
	if mimetypeDetector == nil {
		mimetypeDetector = filesystem.NewMimeTypeDetector()
	}
	return &MemoryFileSystem{
		mu:               new(sync.RWMutex),
		root:             newDir("/", visibility, nil),
		visibility:       visibility,
		mimetypeDetector: mimetypeDetector,
	}
}

func (f *MemoryFileSystem) preparePath(path string) string {
	path = filepath.Clean(path)
	path = filepath.Join(path)
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	return path
}

func (f *MemoryFileSystem) Exists(path string) (bool, error) {
	entry := f.searchEntry(path)
	return entry != nil, nil
}

func (f *MemoryFileSystem) FileExists(path string) (bool, error) {
	entry := f.searchEntry(path)
	return entry != nil && !entry.IsDir(), nil
}

func (f *MemoryFileSystem) DirExists(path string) (bool, error) {
	entry := f.searchEntry(path)
	return entry != nil && entry.IsDir(), nil
}

func (f *MemoryFileSystem) Read(path string) ([]byte, error) {
	if exists, _ := f.FileExists(path); !exists {
		return nil, filesystem.NewUnableToReadFile(path, os.ErrNotExist)
	}
	entry := f.searchEntry(path)
	return entry.read()
}

func (f *MemoryFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	content, err := f.Read(path)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(string(content))), nil
}

func (f *MemoryFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	entry := f.searchEntry(path)
	if entry == nil {
		return nil, filesystem.NewUnableToReadDirectory(path, os.ErrNotExist)
	}
	if !entry.IsDir() {
		return nil, filesystem.NewUnableToReadDirectory(path, filesystem.ErrIsNotDirectory)
	}
	for _, entry := range entry.readDir() {
		if entry.IsDir() {
			entry.visibility = f.visibility
		}
	}
	var entries []os.DirEntry
	for _, entry := range entry.readDir() {
		entries = append(entries, entry)
	}
	return entries, nil
}

func (f *MemoryFileSystem) WalkDir(path string, walkFn gofs.WalkDirFunc) error {
	if exists, _ := f.DirExists(path); !exists {
		return filesystem.NewUnableToReadDirectory(path, os.ErrNotExist)
	}
	root := f.searchEntry(path)
	var walkDir func(path string, d os.DirEntry, fn func(path string, d os.DirEntry, err error) error) error
	walkDir = func(path string, d os.DirEntry, fn func(path string, d os.DirEntry, err error) error) error {
		if err := fn(path, d, nil); err != nil || !d.IsDir() {
			if errors.Is(err, filepath.SkipDir) && d.IsDir() {
				err = nil
			}
			return err
		}
		dirs, err := f.ReadDir(path)
		if err != nil {
			err = fn(path, d, err)
			if err != nil {
				if errors.Is(err, filepath.SkipDir) && d.IsDir() {
					err = nil
				}
				return err
			}
		}
		for _, d1 := range dirs {
			path1 := filepath.Join(path, d1.Name())
			path1 = filepath.ToSlash(path1)
			if err := walkDir(path1, d1, fn); err != nil {
				if errors.Is(err, filepath.SkipDir) {
					break
				}
				return err
			}
		}
		return nil
	}
	err := walkDir(path, root, walkFn)
	if errors.Is(err, filepath.SkipDir) || errors.Is(err, filepath.SkipAll) {
		return nil
	}
	return err
}

func (f *MemoryFileSystem) LastModified(location string) (time.Time, error) {
	entry := f.searchEntry(location)
	if entry == nil {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(location, errors.New("not found"))
	}
	return entry.ModTime(), nil
}

func (f *MemoryFileSystem) FileSize(location string) (int64, error) {
	entry := f.searchEntry(location)
	if entry == nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(location, errors.New("not found"))
	}
	return entry.Size(), nil
}

func (f *MemoryFileSystem) MimeType(path string) (string, error) {
	entry := f.searchEntry(path)
	if entry == nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, errors.New("not found"))
	}
	content, _ := entry.read()
	return f.mimetypeDetector.Detect(path, content), nil
}

func (f *MemoryFileSystem) Visibility(location string) (string, error) {
	entry := f.searchEntry(location)
	if entry == nil {
		return "", filesystem.NewUnableToRetrieveMetadata(location, errors.New("not found"))
	}
	return entry.visibility, nil
}

func (f *MemoryFileSystem) Write(location string, content []byte, config fs.CreateFileConfig) error {
	return f.WriteStream(location, bytes.NewReader(content), config)
}

func (f *MemoryFileSystem) WriteStream(location string, stream io.Reader, config fs.CreateFileConfig) error {
	path := f.preparePath(location)
	if path == "." || path == "/" || path == "./" || path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	var dirVisibility = f.visibility
	var fileVisibility = f.visibility
	var fileFlag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if config != nil {
		dirVisibility = config.DirVisibility()
		fileVisibility = config.FileVisibility()
		fileFlag = config.WriteFlag()
	}
	var dirEntry *dirEntry
	if len(parts) == 1 {
		entry := f.root.findEntry(parts[0])
		if entry == nil {
			entry = f.root.createFile(path, fileVisibility)
		}
		if entry.IsDir() {
			return filesystem.NewUnableToWriteFile(location, errors.New("not a file"))
		}
	}
	dirEntry, err := f.mkdirAll(filepath.Dir(path), dirVisibility)
	if err != nil {
		return err
	}
	filename := filepath.Base(path)
	if entry := dirEntry.findEntry(filename); entry != nil {
		if entry.IsDir() {
			return filesystem.NewUnableToWriteFile(location, errors.New("directory already exists"))
		}
		if err := entry.writeStream(stream, fileFlag&os.O_APPEND > 0); err != nil {
			return filesystem.NewUnableToWriteFile(location, err)
		}
		return nil
	}
	entry := dirEntry.createFile(filename, fileVisibility)
	if err := entry.writeStream(stream, fileFlag&os.O_APPEND > 0); err != nil {
		return filesystem.NewUnableToWriteFile(location, err)
	}
	return nil
}

func (f *MemoryFileSystem) SetVisibility(location string, visibility string) error {
	entry := f.searchEntry(location)
	if entry == nil {
		return filesystem.NewUnableToSetPermission(location, errors.New("not found"))
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.visibility = visibility
	return nil
}

func (f *MemoryFileSystem) Delete(path string) error {
	return f.deleteAll(path, true)
}

func (f *MemoryFileSystem) DeleteDir(location string) error {
	path := f.preparePath(location)
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return nil
	}
	return f.deleteAll(path, false)
}

func (f *MemoryFileSystem) CreateDir(path string, config fs.CreateDirectoryConfig) error {
	var visibility = f.visibility
	if config != nil {
		visibility = config.DirVisibility()
	}
	if _, err := f.mkdirAll(path, visibility); err != nil {
		return err
	}
	return nil
}

func (f *MemoryFileSystem) Move(src string, dst string, config fs.CreateDirectoryConfig) error {
	var dirVisibility = f.visibility
	if config != nil {
		dirVisibility = config.DirVisibility()
	}
	srcEntry := f.searchEntry(src)
	if srcEntry == nil {
		return filesystem.NewUnableToMove(src, dst, os.ErrNotExist)
	}
	dstEntry := f.searchEntry(dst)
	if dstEntry != nil {
		return filesystem.NewUnableToMove(dst, src, os.ErrExist)
	}
	var err error
	var dstDir *dirEntry
	if srcEntry.IsDir() {
		dstDir, err = f.mkdirAll(dst, dirVisibility)
		if err != nil {
			return filesystem.NewUnableToMove(src, dst, err)
		}
		if err := f.deleteAll(src, false); err != nil {
			return filesystem.NewUnableToMove(src, dst, err)
		}
	} else {
		dstDir, err = f.mkdirAll(filepath.Dir(dst), dirVisibility)
		if err != nil {
			return filesystem.NewUnableToMove(src, dst, err)
		}
		if err := f.deleteAll(src, true); err != nil {
			return filesystem.NewUnableToMove(src, dst, err)
		}
	}
	srcEntry.name = filepath.Base(dst)
	dstDir.addEntry(srcEntry)
	return nil
}

func (f *MemoryFileSystem) Copy(src string, dst string, config fs.CreateFileConfig) error {
	var dirVisibility = f.visibility
	var fileVisibility = f.visibility
	var fileFlag int
	if config != nil {
		dirVisibility = config.DirVisibility()
		fileVisibility = config.FileVisibility()
		fileFlag = config.WriteFlag()
	}
	srcEntry := f.searchEntry(src)
	if srcEntry == nil {
		return filesystem.NewUnableToCopyFile(src, dst, os.ErrNotExist)
	}
	if srcEntry.IsDir() {
		return filesystem.NewUnableToCopyFile(src, dst, filesystem.ErrIsNotFile)
	}
	dstEntry := f.searchEntry(dst)
	if dstEntry != nil && fileFlag&os.O_TRUNC <= 0 {
		return filesystem.NewUnableToCopyFile(dst, src, os.ErrExist)
	}
	dstEntry, err := f.mkdirAll(filepath.Dir(dst), dirVisibility)
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	copyEntry := newFile(filepath.Base(dst), fileVisibility, nil)
	srcEntry.copyTo(copyEntry)
	dstEntry.addEntry(copyEntry)
	return nil
}

func (f *MemoryFileSystem) searchEntry(path string) *dirEntry {
	path = f.preparePath(path)
	if path == "/" || path == "" || path == "." || path == "./" {
		return f.root
	}
	parts := strings.Split(path, "/")
	var currentEntry = f.root
	for _, part := range parts {
		var entry = currentEntry.findEntry(part)
		if entry == nil {
			return nil
		}
		currentEntry = entry
	}
	return currentEntry
}

func (f *MemoryFileSystem) deleteAll(path string, file bool) error {
	entry := f.searchEntry(path)
	if entry == nil {
		return nil
	}
	if file && entry.IsDir() {
		return filesystem.NewUnableToDeleteFile(path, errors.New("not a file"))
	}
	if !file && !entry.IsDir() {
		return filesystem.NewUnableToDeleteFile(path, errors.New("not a directory"))
	}
	if entry.parent == nil {
		f.root.entries = make(map[string]*dirEntry)
	} else {
		entry.parent.deleteEntry(entry.name)
		entry.parent = nil
	}
	return nil
}

func (f *MemoryFileSystem) mkdirAll(path string, visibility string) (*dirEntry, error) {
	var currentEntry = f.root
	path = f.preparePath(path)
	if path == "/" || path == "" || path == "." || path == "./" {
		return f.root, nil
	}
	parts := strings.Split(path, "/")
	for _, part := range parts {
		var entry *dirEntry
		entry = currentEntry.findEntry(part)
		if entry == nil {
			entry = newDir(part, visibility, currentEntry)
			currentEntry = currentEntry.createDir(part, visibility)
		} else {
			if !entry.IsDir() {
				return nil, filesystem.NewUnableToCreateDirectory(path, errors.New("file exists"))
			}
			currentEntry = entry
		}
	}
	return currentEntry, nil
}
