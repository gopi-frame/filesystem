package sftp

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gopi-frame/filesystem/visibility/unix"

	fs2 "github.com/gopi-frame/contract/filesystem"

	"github.com/gopi-frame/filesystem"
)

type SFTPFileSystem struct {
	clientPool          ClientPool
	mimeTypeDetector    fs2.MimeTypeDetector
	visibilityConvertor unix.VisibilityConvertor
}

func NewSFTPFileSystem(config *Config, opts ...Option) (*SFTPFileSystem, error) {
	f := &SFTPFileSystem{
		clientPool:          newClientPool(config),
		mimeTypeDetector:    filesystem.NewMimeTypeDetector(),
		visibilityConvertor: unix.New(),
	}
	for _, opt := range opts {
		if err := opt.Apply(f); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (fs *SFTPFileSystem) Exists(path string) (bool, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	_, err = client.SFTPClient().Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return true, nil
}

func (fs *SFTPFileSystem) FileExists(path string) (bool, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return info.Mode().IsRegular(), nil
}

func (fs *SFTPFileSystem) DirExists(path string) (bool, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return info.IsDir(), nil
}

func (fs *SFTPFileSystem) Read(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	content, err := io.ReadAll(f)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	if err != nil {
		return content, err
	}
	return content, nil
}

func (fs *SFTPFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	file, err := client.SFTPClient().Open(path)
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	return file, nil
}

func (fs *SFTPFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	entries, err := client.SFTPClient().ReadDir(path)
	if err != nil {
		return nil, filesystem.NewUnableToReadDirectory(path, err)
	}
	var dirEntries []os.DirEntry
	for _, entry := range entries {
		dirEntries = append(dirEntries, &dirEntry{entry})
	}
	return dirEntries, nil
}

func (fs *SFTPFileSystem) WalkDir(path string, walkFn fs.WalkDirFunc) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToReadDirectory(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	w := client.SFTPClient().Walk(path)
	for w.Step() {
		if w.Stat().Mode()&os.ModeSymlink != 0 {
			continue
		}
		if err := walkFn(w.Path(), &dirEntry{w.Stat()}, err); err != nil {
			if errors.Is(err, filepath.SkipDir) || errors.Is(err, filepath.SkipAll) {
				err = nil
			}
		}
		if w.Err() != nil && !errors.Is(w.Err(), io.EOF) {
			return filesystem.NewUnableToReadDirectory(path, w.Err())
		}
	}
	if err != nil {
		return filesystem.NewUnableToReadDirectory(path, err)
	}
	return nil
}

func (fs *SFTPFileSystem) LastModified(path string) (time.Time, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	return info.ModTime(), nil
}

func (fs *SFTPFileSystem) FileSize(path string) (int64, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	return info.Size(), nil
}

func (fs *SFTPFileSystem) MimeType(path string) (string, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	stream, err := client.SFTPClient().Open(path)
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	buffer := make([]byte, 3072)
	n, err := stream.Read(buffer)
	if err1 := stream.Close(); err1 != nil && err == nil {
		err = err1
	}
	mimeType := fs.mimeTypeDetector.Detect(path, buffer[:n])
	if err != nil {
		return mimeType, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	return mimeType, nil
}

func (fs *SFTPFileSystem) Visibility(path string) (string, error) {
	client, err := fs.clientPool.Get()
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	if info.IsDir() {
		return fs.visibilityConvertor.InverseForDir(info.Mode().Perm()), nil
	}
	return fs.visibilityConvertor.InverseForFile(info.Mode().Perm()), nil
}

func (fs *SFTPFileSystem) Write(path string, content []byte, config fs2.CreateFileConfig) error {
	return fs.WriteStream(path, bytes.NewReader(content), config)
}

func (fs *SFTPFileSystem) WriteStream(path string, stream io.Reader, config fs2.CreateFileConfig) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	var dirMode = fs.visibilityConvertor.DefaultForDir()
	var fileMode = fs.visibilityConvertor.DefaultForFile()
	var writeFlag = os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	if config != nil {
		dirMode = fs.visibilityConvertor.ForDir(config.DirVisibility())
		fileMode = fs.visibilityConvertor.ForFile(config.FileVisibility())
		writeFlag = config.WriteFlag()
	}
	if err := client.SFTPClient().MkdirAll(filepath.ToSlash(filepath.Dir(path))); err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	if err := client.SFTPClient().Chmod(filepath.ToSlash(filepath.Dir(path)), dirMode); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	file, err := client.SFTPClient().OpenFile(path, writeFlag)
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			//TODO: error handle
		}
	}()
	if _, err := io.Copy(file, stream); err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	if err := client.SFTPClient().Chmod(path, fileMode); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

func (fs *SFTPFileSystem) SetVisibility(path string, visibility string) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		return filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	var perm os.FileMode
	if info.IsDir() {
		perm = fs.visibilityConvertor.ForDir(visibility)
	} else {
		perm = fs.visibilityConvertor.ForFile(visibility)
	}
	if err := client.SFTPClient().Chmod(path, perm); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

func (fs *SFTPFileSystem) Delete(path string) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	if info.IsDir() {
		return filesystem.NewUnableToDeleteFile(path, filesystem.ErrIsNotFile)
	} else {
		if err := client.SFTPClient().Remove(path); err != nil {
			return filesystem.NewUnableToDeleteFile(path, err)
		}
	}
	return nil
}

func (fs *SFTPFileSystem) DeleteDir(path string) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToDeleteDirectory(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	info, err := client.SFTPClient().Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return filesystem.NewUnableToDeleteDirectory(path, err)
	}
	if !info.IsDir() {
		return filesystem.NewUnableToDeleteDirectory(path, filesystem.ErrIsNotDirectory)
	}
	if err := client.SFTPClient().RemoveDirectory(path); err != nil {
		return filesystem.NewUnableToDeleteDirectory(path, err)
	}
	return nil
}

func (fs *SFTPFileSystem) CreateDir(path string, config fs2.CreateDirectoryConfig) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	defer fs.clientPool.Put(client)
	path = filepath.ToSlash(filepath.Clean(path))
	var dirMode = fs.visibilityConvertor.DefaultForDir()
	if config != nil {
		dirMode = fs.visibilityConvertor.ForDir(config.DirVisibility())
	}
	if err := client.SFTPClient().MkdirAll(path); err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	if err := client.SFTPClient().Chmod(path, dirMode); err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

func (fs *SFTPFileSystem) Move(src string, dst string, config fs2.CreateDirectoryConfig) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	defer fs.clientPool.Put(client)
	src = filepath.ToSlash(filepath.Clean(src))
	dst = filepath.ToSlash(filepath.Clean(dst))
	_, err = client.SFTPClient().Stat(src)
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	_, err = client.SFTPClient().Stat(dst)
	if err == nil {
		return filesystem.NewUnableToMove(src, dst, os.ErrExist)
	}
	var dirMode = fs.visibilityConvertor.DefaultForDir()
	if config != nil {
		dirMode = fs.visibilityConvertor.ForDir(config.DirVisibility())
	}
	if err := client.SFTPClient().MkdirAll(filepath.Dir(dst)); err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	if err := client.SFTPClient().Chmod(dst, dirMode); err != nil {
		return filesystem.NewUnableToSetPermission(dst, err)
	}
	if err := client.SFTPClient().Rename(src, dst); err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	return nil
}

func (fs *SFTPFileSystem) Copy(src string, dst string, config fs2.CreateFileConfig) error {
	client, err := fs.clientPool.Get()
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	defer fs.clientPool.Put(client)
	var dirMode = fs.visibilityConvertor.DefaultForDir()
	var fileMode = fs.visibilityConvertor.DefaultForFile()
	var writeFlag = os.O_CREATE | os.O_WRONLY
	if config != nil {
		dirMode = fs.visibilityConvertor.ForDir(config.DirVisibility())
		fileMode = fs.visibilityConvertor.ForFile(config.FileVisibility())
		writeFlag = config.WriteFlag()
	}
	src = filepath.ToSlash(filepath.Clean(src))
	dst = filepath.ToSlash(filepath.Clean(dst))
	srcFile, err := client.SFTPClient().OpenFile(src, os.O_RDONLY)
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	defer srcFile.Close()
	dstStat, err := client.SFTPClient().Stat(dst)
	if err == nil && writeFlag&os.O_TRUNC == 0 {
		return filesystem.NewUnableToCopyFile(src, dst, os.ErrExist)
	} else if err == nil && dstStat.IsDir() {
		return filesystem.NewUnableToCopyFile(src, dst, filesystem.ErrIsNotFile)
	}
	if err := client.SFTPClient().MkdirAll(filepath.Dir(dst)); err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	if err := client.SFTPClient().Chmod(filepath.Dir(dst), dirMode); err != nil {
		return filesystem.NewUnableToSetPermission(filepath.Dir(dst), err)
	}
	dstFile, err := client.SFTPClient().OpenFile(dst, writeFlag)
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	if err := client.SFTPClient().Chmod(dst, fileMode); err != nil {
		return filesystem.NewUnableToSetPermission(dst, err)
	}
	return nil
}
