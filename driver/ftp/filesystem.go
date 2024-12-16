package ftp

import (
	"bytes"
	"errors"
	"io"
	gofs "io/fs"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gopi-frame/filesystem/visibility/unix"

	fs "github.com/gopi-frame/contract/filesystem"

	"github.com/gopi-frame/filesystem"

	"github.com/gopi-frame/ftp"
)

type FTPFileSystem struct {
	connPool            ConnPool
	mimetypeDetector    fs.MimeTypeDetector
	visibilityConvertor unix.VisibilityConvertor
}

func NewFTPFileSystem(config *Config, opts ...Option) (*FTPFileSystem, error) {
	f := &FTPFileSystem{
		connPool:            newConnPool(config),
		mimetypeDetector:    filesystem.NewMimeTypeDetector(),
		visibilityConvertor: unix.New(),
	}
	for _, opt := range opts {
		if err := opt.Apply(f); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (f *FTPFileSystem) getEntry(conn *ftp.ServerConn, name string) (*ftp.Entry, error) {
	name = filepath.ToSlash(name)
	entry, err := conn.GetEntry(name)
	if err == nil {
		return entry, nil
	}
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) && protoErr.Code == ftp.StatusNotImplemented {
		entries, err := conn.List(filepath.ToSlash(filepath.Dir(name)))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Name() == filepath.Base(name) {
				return entry, nil
			}
		}
		return nil, os.ErrNotExist
	}
	return nil, err
}

// Exists returns true if the file or directory exists.
func (f *FTPFileSystem) Exists(path string) (bool, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	defer f.connPool.Put(conn)
	_, err = f.getEntry(conn, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return true, nil
}

// FileExists returns true if the file exists.
func (f *FTPFileSystem) FileExists(path string) (bool, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else {
			return false, filesystem.NewUnableToCheckExistence(path, err)
		}
	}
	return entry.FileMode.IsRegular(), nil
}

// DirExists returns true if the directory exists.
func (f *FTPFileSystem) DirExists(path string) (bool, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else {
			return false, filesystem.NewUnableToCheckExistence(path, err)
		}
	}
	return entry.IsDir(), nil
}

// Read returns the content of the file.
func (f *FTPFileSystem) Read(path string) ([]byte, error) {
	reader, err := f.ReadStream(path)
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(reader)
	if err1 := reader.Close(); err1 != nil && err == nil {
		return content, err1
	}
	return content, nil
}

// ReadStream returns the content of the file as a stream.
func (f *FTPFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	defer f.connPool.Put(conn)
	if entry, err := f.getEntry(conn, path); err != nil {
		return nil, err
	} else if !entry.Type().IsRegular() {
		return nil, filesystem.NewUnableToReadFile(path, filesystem.ErrIsNotFile)
	}
	resp, err := conn.Retr(path)
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, resp)
	if err1 := resp.Close(); err1 != nil {
		err = err1
	}
	if err != nil {
		return io.NopCloser(buf), filesystem.NewUnableToReadFile(path, err)
	}
	return io.NopCloser(buf), nil
}

// ReadDir returns the content of the directory.
func (f *FTPFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return nil, filesystem.NewUnableToReadDirectory(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		return nil, filesystem.NewUnableToReadDirectory(path, err)
	}
	if !entry.IsDir() {
		return nil, filesystem.NewUnableToReadDirectory(path, filesystem.ErrIsNotDirectory)
	}
	entries, err := conn.List(filepath.ToSlash(path))
	if err != nil {
		return nil, filesystem.NewUnableToReadDirectory(path, err)
	}
	var files []os.DirEntry
	for _, entry := range entries {
		files = append(files, entry)
	}
	return files, nil
}

// WalkDir walks the file tree rooted at root, calling walkFn for each file or directory in the tree.
// This function does not follow symbolic links.
func (f *FTPFileSystem) WalkDir(path string, walkFn gofs.WalkDirFunc) error {
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToReadFile(path, err)
	}
	defer f.connPool.Put(conn)
	w := conn.Walk(path)
	for w.Next() {
		if w.Stat().Type()&os.ModeSymlink != 0 {
			continue
		}
		if err = walkFn(w.Path(), w.Stat(), err); err != nil {
			if errors.Is(err, filepath.SkipDir) || errors.Is(err, filepath.SkipAll) {
				err = nil
			}
		}
		if w.Err() != nil && !errors.Is(w.Err(), io.EOF) {
			return filesystem.NewUnableToReadDirectory(path, w.Err())
		}
	}
	if err != nil {
		return filesystem.NewUnableToReadFile(path, err)
	}
	return nil
}

// LastModified returns the last modified time of the file.
func (f *FTPFileSystem) LastModified(path string) (time.Time, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	if !entry.Type().IsRegular() {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, filesystem.ErrIsNotFile)
	}
	if conn.IsGetTimeSupported() {
		if t, err := conn.GetTime(path); err == nil {
			return t, nil
		} else {
			return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
		}
	}
	return entry.Time, nil
}

// FileSize returns the size of the file.
func (f *FTPFileSystem) FileSize(path string) (int64, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	if !entry.Type().IsRegular() {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, filesystem.ErrIsNotFile)
	}
	filesize, err := conn.FileSize(path)
	if err != nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	return filesize, nil
}

// MimeType returns the mime type of the file.
func (f *FTPFileSystem) MimeType(path string) (string, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	if !entry.FileMode.IsRegular() {
		return "", filesystem.NewUnableToRetrieveMetadata(path, filesystem.ErrIsNotFile)
	}
	resp, err := conn.Retr(path)
	if err == nil {
		buf := make([]byte, 3072)
		_, err = resp.Read(buf)
		if err1 := resp.Close(); err1 != nil {
			err = err1
		}
		return f.mimetypeDetector.Detect(path, buf), nil
	}
	return f.mimetypeDetector.DetectFromPath(path), nil
}

// Visibility returns the visibility of the file.
func (f *FTPFileSystem) Visibility(path string) (string, error) {
	conn, err := f.connPool.Get()
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	if entry.IsDir() {
		return f.visibilityConvertor.InverseForDir(entry.FileMode), nil
	}
	return f.visibilityConvertor.InverseForFile(entry.FileMode), nil
}

// Write writes the content to the file.
func (f *FTPFileSystem) Write(path string, content []byte, config fs.CreateFileConfig) error {
	return f.WriteStream(path, bytes.NewReader(content), config)
}

// WriteStream writes the content to the file.
// If the file already exists, it will be overwritten unless the config.WriteFlag() is set to os.O_APPEND.
func (f *FTPFileSystem) WriteStream(path string, content io.Reader, config fs.CreateFileConfig) error {
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	defer f.connPool.Put(conn)
	var dirMode = f.visibilityConvertor.DefaultForDir()
	var fileMode = f.visibilityConvertor.DefaultForFile()
	var writeFlag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if config != nil {
		dirMode = f.visibilityConvertor.ForDir(config.DirVisibility())
		fileMode = f.visibilityConvertor.ForFile(config.FileVisibility())
		writeFlag = config.WriteFlag()
	}
	entry, err := f.getEntry(conn, path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	if entry != nil && !entry.Type().IsRegular() {
		return filesystem.NewUnableToWriteFile(path, filesystem.ErrIsNotFile)
	}
	dir := filepath.Dir(path)
	if err := f.mkdirAll(conn, dir, dirMode); err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	if writeFlag&os.O_APPEND > 0 {
		if err := conn.Append(path, content); err != nil {
			return filesystem.NewUnableToWriteFile(path, err)
		}
	} else {
		if err := conn.Stor(path, content); err != nil {
			return filesystem.NewUnableToWriteFile(path, err)
		}
	}
	if err := f.setPermission(conn, path, fileMode); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

func (f *FTPFileSystem) mkdirAll(conn *ftp.ServerConn, path string, mode os.FileMode) error {
	path = filepath.ToSlash(filepath.Clean(path))
	parts := strings.Split(path, "/")
	var prefix string
	if parts[0] == "/" {
		prefix = "/"
		parts = parts[1:]
	}
	for i := 0; i < len(parts); i++ {
		if parts[i] == "." {
			continue
		}
		if parts[i] == ".." {
			err := conn.ChangeDirToParent()
			if err != nil {
				return err
			}
		}
		fp := filepath.ToSlash(filepath.Join(prefix, parts[i]))
		entry, err := f.getEntry(conn, fp)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		} else if errors.Is(err, os.ErrNotExist) {
			if err := conn.MakeDir(fp); err != nil {
				if !errors.Is(err, os.ErrExist) {
					return err
				}
			}
			if err := f.setPermission(conn, fp, mode); err != nil {
				return err
			}
		} else {
			if entry != nil {
				if !entry.IsDir() {
					return filesystem.ErrIsNotDirectory
				}
			}
		}
		prefix = filepath.ToSlash(filepath.Join(prefix, parts[i]))
	}
	return nil
}

// SetVisibility sets the visibility of the file.
func (f *FTPFileSystem) SetVisibility(path string, visibility string) error {
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	var perm os.FileMode
	if entry.IsDir() {
		perm = f.visibilityConvertor.ForDir(visibility)
	} else {
		perm = f.visibilityConvertor.ForFile(visibility)
	}
	if err := f.setPermission(conn, path, perm); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

func (f *FTPFileSystem) setPermission(conn *ftp.ServerConn, path string, perm os.FileMode) error {
	if err := conn.ChangeMode(path, perm); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

// Delete deletes the file or does nothing if the file does not exist.
func (f *FTPFileSystem) Delete(path string) error {
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	if !entry.Type().IsRegular() {
		return filesystem.NewUnableToDeleteFile(path, filesystem.ErrIsNotFile)
	}
	if err := conn.Delete(path); err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	return nil
}

// DeleteDir deletes the directory and all its contents.
func (f *FTPFileSystem) DeleteDir(path string) error {
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	if !entry.IsDir() {
		return filesystem.NewUnableToDeleteFile(path, filesystem.ErrIsNotDirectory)
	}
	if err := conn.RemoveDirRecur(path); err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	return nil
}

// CreateDir creates a directory.
func (f *FTPFileSystem) CreateDir(path string, config fs.CreateDirectoryConfig) error {
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	defer f.connPool.Put(conn)
	entry, err := f.getEntry(conn, path)
	if err == nil && !entry.IsDir() {
		return filesystem.NewUnableToCreateDirectory(path, filesystem.ErrIsNotDirectory)
	}
	dirMode := f.visibilityConvertor.DefaultForDir()
	if config != nil {
		dirMode = f.visibilityConvertor.ForDir(config.DirVisibility())
	}
	if err := f.mkdirAll(conn, path, dirMode); err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	return nil
}

// Move moves a file or directory to a new location.
// If the destination file or directory already exists, the operation fails.
func (f *FTPFileSystem) Move(src string, dst string, config fs.CreateDirectoryConfig) error {
	src = filepath.ToSlash(filepath.Clean(src))
	dst = filepath.ToSlash(filepath.Clean(dst))
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	defer f.connPool.Put(conn)
	_, err = f.getEntry(conn, src)
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	_, err = f.getEntry(conn, dst)
	if err == nil {
		return filesystem.NewUnableToMove(dst, src, os.ErrExist)
	}
	dirMode := f.visibilityConvertor.DefaultForDir()
	if config != nil {
		dirMode = f.visibilityConvertor.ForDir(config.DirVisibility())
	}
	if err := f.mkdirAll(conn, filepath.ToSlash(filepath.Dir(dst)), dirMode); err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	if err := conn.Rename(src, dst); err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	return nil
}

// Copy copies a file to a new location.
// If the destination already exists, the operation fails unless config.WriteFlag() is set to os.O_APPEND or os.O_TRUNC.
//
// NOTICE:
//
//	when the write flag is set to os.O_TRUNC, the original destination will be deleted first,
//	if any error occurs after the file is deleted, the destination will be destroyed.
//	so it is recommended to not set the write flag to os.O_TRUNC.
func (f *FTPFileSystem) Copy(src string, dst string, config fs.CreateFileConfig) error {
	src = filepath.ToSlash(filepath.Clean(src))
	dst = filepath.ToSlash(filepath.Clean(dst))
	conn, err := f.connPool.Get()
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	defer f.connPool.Put(conn)
	srcEntry, err := f.getEntry(conn, src)
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	if !srcEntry.FileMode.IsRegular() {
		return filesystem.NewUnableToCopyFile(src, dst, filesystem.ErrIsNotFile)
	}
	dirMode := f.visibilityConvertor.DefaultForDir()
	fileMode := f.visibilityConvertor.DefaultForFile()
	writeFlag := os.O_WRONLY | os.O_CREATE
	if config != nil {
		dirMode = f.visibilityConvertor.ForDir(config.DirVisibility())
		fileMode = f.visibilityConvertor.ForFile(config.FileVisibility())
		writeFlag = config.WriteFlag()
	}
	dstEntry, err := f.getEntry(conn, dst)
	if err == nil && dstEntry != nil {
		if dstEntry.IsDir() {
			return filesystem.NewUnableToCopyFile(src, dst, os.ErrExist)
		}
		if writeFlag&os.O_TRUNC == 0 && writeFlag&os.O_APPEND == 0 {
			return filesystem.NewUnableToCopyFile(src, dst, os.ErrExist)
		}
	}
	srcStream, err := conn.Retr(src)
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, srcStream)
	_ = srcStream.Close()
	if dstEntry != nil && writeFlag&os.O_TRUNC != 0 {
		if err := conn.Delete(dst); err != nil {
			return filesystem.NewUnableToCopyFile(src, dst, err)
		}
	}
	if err := f.mkdirAll(conn, filepath.ToSlash(filepath.Dir(dst)), dirMode); err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	if writeFlag&os.O_APPEND != 0 {
		if err := conn.Append(dst, buf); err != nil {
			return filesystem.NewUnableToCopyFile(src, dst, err)
		}
	} else {
		if err := conn.Stor(dst, buf); err != nil {
			return filesystem.NewUnableToCopyFile(src, dst, err)
		}
		if err := f.setPermission(conn, dst, fileMode); err != nil {
			return filesystem.NewUnableToCopyFile(src, dst, err)
		}
	}
	return nil
}
