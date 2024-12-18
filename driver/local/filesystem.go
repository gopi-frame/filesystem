package local

import (
	"bytes"
	"io"
	gofs "io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gopi-frame/filesystem/visibility/unix"

	"github.com/gopi-frame/contract"

	"github.com/gopi-frame/exception"

	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

var ErrEmptyRoot = exception.New("empty root")

type LocalFileSystem struct {
	root string

	createRootOnce      sync.Once
	deferRootCreation   bool
	mimetypeDetector    fs.MimeTypeDetector
	visibilityConvertor unix.VisibilityConvertor
}

func NewLocalFileSystem(root string, opts ...contract.Option[*LocalFileSystem]) (*LocalFileSystem, error) {
	if root == "" {
		panic(ErrEmptyRoot)
	}
	f := &LocalFileSystem{
		root:                root,
		mimetypeDetector:    filesystem.NewMimeTypeDetector(),
		visibilityConvertor: unix.New(),
	}
	for _, opt := range opts {
		if err := opt.Apply(f); err != nil {
			return nil, err
		}
	}
	if !f.deferRootCreation {
		if err := f.createRoot(); err != nil {
			return nil, filesystem.NewUnableToCreateDirectory(f.root, err)
		}
	}
	return f, nil
}

func (f *LocalFileSystem) createRoot() (err error) {
	f.createRootOnce.Do(func() {
		err = os.MkdirAll(f.root, os.ModePerm)
	})
	if err != nil {
		return filesystem.NewUnableToCreateDirectory(f.root, err)
	}
	return nil
}

func (f *LocalFileSystem) setPermission(path string, mode os.FileMode) error {
	file := filepath.Join(f.root, path)
	if err := os.Chmod(file, mode); err != nil {
		return filesystem.NewUnableToSetPermission(file, err)
	}
	return nil
}

func (f *LocalFileSystem) Exists(path string) (bool, error) {
	file := filepath.Join(f.root, path)
	if _, err := os.Stat(file); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
}

func (f *LocalFileSystem) FileExists(path string) (bool, error) {
	file := filepath.Join(f.root, path)
	stat, err := os.Stat(file)
	if err == nil {
		return !stat.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, filesystem.NewUnableToCheckExistence(path, err)
}

func (f *LocalFileSystem) DirExists(path string) (bool, error) {
	file := filepath.Join(f.root, path)
	stat, err := os.Stat(file)
	if err == nil {
		return stat.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, filesystem.NewUnableToCheckExistence(path, err)
}

func (f *LocalFileSystem) Read(path string) ([]byte, error) {
	file := filepath.Join(f.root, path)
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	return content, nil
}

func (f *LocalFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	fp := filepath.Join(f.root, path)
	file, err := os.Open(fp)
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	return file, nil
}

func (f *LocalFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	dir := filepath.Join(f.root, path)
	if exists, _ := f.DirExists(dir); !exists {
		return nil, nil
	}
	return os.ReadDir(dir)
}

func (f *LocalFileSystem) WalkDir(path string, walkFn gofs.WalkDirFunc) error {
	dir := filepath.Join(f.root, path)
	if exists, _ := f.DirExists(dir); !exists {
		return filesystem.NewUnableToReadDirectory(path, os.ErrNotExist)
	}
	return filepath.WalkDir(dir, walkFn)
}

func (f *LocalFileSystem) LastModified(path string) (time.Time, error) {
	if stat, err := os.Stat(filepath.Join(f.root, path)); err == nil {
		return stat.ModTime(), nil
	} else {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
}

func (f *LocalFileSystem) FileSize(path string) (int64, error) {
	if stat, err := os.Stat(filepath.Join(f.root, path)); err == nil {
		return stat.Size(), nil
	} else {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
}

func (f *LocalFileSystem) MimeType(path string) (string, error) {
	var detector = f.mimetypeDetector
	if detector == nil {
		detector = filesystem.NewMimeTypeDetector()
	}
	return detector.DetectFromFile(filepath.Join(f.root, path)), nil
}

func (f *LocalFileSystem) Visibility(path string) (string, error) {
	if stat, err := os.Stat(filepath.Join(f.root, path)); err == nil {
		mode := stat.Mode() & os.ModePerm
		if stat.IsDir() {
			return f.visibilityConvertor.InverseForDir(mode), nil
		}
		return f.visibilityConvertor.InverseForFile(mode), nil
	} else {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
}

func (f *LocalFileSystem) Write(path string, content []byte, config map[string]any) error {
	return f.WriteStream(path, bytes.NewReader(content), config)
}

func (f *LocalFileSystem) WriteStream(path string, stream io.Reader, config map[string]any) error {
	if err := f.createRoot(); err != nil {
		return err
	}
	fp := filepath.Join(f.root, path)
	var dirMode = f.visibilityConvertor.DefaultForDir()
	var fileMode = f.visibilityConvertor.DefaultForFile()
	var fileFlag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return err
		}
		if cfg.DirVisibility != nil {
			dirMode = f.visibilityConvertor.ForDir(*cfg.DirVisibility)
		}
		if cfg.FileVisibility != nil {
			fileMode = f.visibilityConvertor.ForFile(*cfg.FileVisibility)
		}
		if cfg.FileWriteFlag != nil {
			fileFlag = *cfg.FileWriteFlag
		}
	}
	if err := os.MkdirAll(filepath.Dir(fp), dirMode); err != nil {
		return filesystem.NewUnableToCreateDirectory(filepath.Dir(fp), err)
	}
	file, err := os.OpenFile(fp, fileFlag, fileMode)
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	_, err = io.Copy(file, stream)
	if err1 := file.Close(); err1 != nil && err == nil {
		return filesystem.NewUnableToCloseFile(path, err1)
	}
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	return nil
}

func (f *LocalFileSystem) SetVisibility(path string, visibility string) error {
	if exists, _ := f.FileExists(path); exists {
		mode := f.visibilityConvertor.ForFile(visibility)
		return f.setPermission(path, mode)
	}
	mode := f.visibilityConvertor.ForDir(visibility)
	return f.setPermission(path, mode)
}

func (f *LocalFileSystem) Delete(path string) error {
	exists, _ := f.FileExists(path)
	if !exists {
		return nil
	}
	if err := os.Remove(filepath.Join(f.root, path)); err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	return nil
}

func (f *LocalFileSystem) DeleteDir(path string) error {
	exists, _ := f.DirExists(path)
	if !exists {
		return nil
	}
	if err := os.RemoveAll(filepath.Join(f.root, path)); err != nil {
		return filesystem.NewUnableToDeleteDirectory(path, err)
	}
	return nil
}

func (f *LocalFileSystem) CreateDir(path string, config map[string]any) error {
	var mode = f.visibilityConvertor.DefaultForDir()
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return err
		}
		if cfg.DirVisibility != nil {
			mode = f.visibilityConvertor.ForDir(*cfg.DirVisibility)
		}
	}
	exists, _ := f.DirExists(path)
	if exists {
		return f.setPermission(path, mode)
	}
	if err := os.MkdirAll(filepath.Join(f.root, path), mode); err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	return nil
}

func (f *LocalFileSystem) Move(src string, dst string, config map[string]any) error {
	if err := f.createRoot(); err != nil {
		return filesystem.NewUnableToCreateDirectory(f.root, err)
	}
	srcPath := filepath.Join(f.root, src)
	dstPath := filepath.Join(f.root, dst)
	var mode = f.visibilityConvertor.DefaultForDir()
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return err
		}
		if cfg.DirVisibility != nil {
			mode = f.visibilityConvertor.ForDir(*cfg.DirVisibility)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), mode); err != nil {
		return filesystem.NewUnableToCreateDirectory(filepath.Dir(dstPath), err)
	}
	if err := os.Rename(srcPath, dstPath); err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	return nil
}

func (f *LocalFileSystem) Copy(src string, dst string, config map[string]any) error {
	if err := f.createRoot(); err != nil {
		return filesystem.NewUnableToCreateDirectory(f.root, err)
	}
	file, err := f.ReadStream(src)
	if err != nil {
		return err
	}
	err = f.WriteStream(dst, file, config)
	if closer, ok := file.(io.Closer); ok {
		if err1 := closer.Close(); err1 != nil && err == nil {
			return filesystem.NewUnableToCloseFile(src, err1)
		}
	}
	if err != nil {
		return err
	}
	return nil
}
