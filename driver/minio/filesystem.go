package minio

import (
	"bytes"
	"context"
	"errors"
	"io"
	gofs "io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/gopi-frame/exception"

	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

type MinioFileSystem struct {
	client           *minio.Client
	bucket           string
	mimeTypeDetector fs.MimeTypeDetector
}

func NewMinioFileSystem(opts ...Option) (*MinioFileSystem, error) {
	f := new(MinioFileSystem)
	f.mimeTypeDetector = filesystem.NewMimeTypeDetector()
	for _, opt := range opts {
		if err := opt.Apply(f); err != nil {
			return nil, err
		}
	}
	if f.client == nil {
		return nil, errors.New("minio client is required")
	}
	return f, nil
}

func (m *MinioFileSystem) Exists(path string) (bool, error) {
	_, err := m.client.StatObject(context.Background(), m.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return true, nil
}

func (m *MinioFileSystem) FileExists(path string) (bool, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return false, nil
	}
	_, err := m.client.StatObject(context.Background(), m.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		if respErr := minio.ToErrorResponse(err); respErr.Code == "NoSuchKey" {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return true, nil
}

func (m *MinioFileSystem) DirExists(path string) (bool, error) {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return false, nil
	}
	_, err := m.client.StatObject(context.Background(), m.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		var notFoundErr *minio.ErrorResponse
		if errors.As(err, &notFoundErr) && notFoundErr.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return true, nil
}

func (m *MinioFileSystem) Read(path string) ([]byte, error) {
	stream, err := m.ReadStream(path)
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	content, err := io.ReadAll(stream)
	if err1 := stream.Close(); err1 != nil && err == nil {
		err = err1
	}
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	return content, nil
}

func (m *MinioFileSystem) ReadStream(path string) (io.ReadCloser, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return nil, filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotFile)
	}
	resp, err := m.client.GetObject(context.Background(), m.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, filesystem.NewUnableToCheckExistence(path, err)
	}
	return resp, nil
}

func (m *MinioFileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return nil, filesystem.NewUnableToReadDirectory(path, filesystem.ErrIsNotDirectory)
	}
	objects := m.client.ListObjects(context.Background(), m.bucket, minio.ListObjectsOptions{
		Prefix: path,
	})
	var entries []os.DirEntry
	for object := range objects {
		if object.Err != nil {
			return nil, filesystem.NewUnableToReadFile(path, object.Err)
		}
		entries = append(entries, &dirEntry{
			obj: object,
		})
	}
	return entries, nil
}

func (m *MinioFileSystem) WalkDir(path string, walkFn gofs.WalkDirFunc) error {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotDirectory)
	}
	objects := m.client.ListObjects(context.Background(), m.bucket, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: true,
	})
	var err error
	var currentDir string
	for object := range objects {
		if object.Err != nil {
			err = object.Err
		}
		if strings.HasSuffix(object.Key, "/") {
			currentDir = object.Key
		}
		if err := walkFn(currentDir, &dirEntry{obj: object}, err); err != nil {
			if errors.Is(err, gofs.SkipDir) || errors.Is(err, gofs.SkipAll) {
				err = nil
			}
		}
	}
	if err != nil {
		return filesystem.NewUnableToReadDirectory(path, err)
	}
	return nil
}

func (m *MinioFileSystem) LastModified(path string) (time.Time, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return time.Time{}, filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotFile)
	}
	object, err := m.client.StatObject(context.Background(), m.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return time.Time{}, filesystem.NewUnableToCheckExistence(path, err)
	}
	return object.LastModified, nil
}

func (m *MinioFileSystem) FileSize(path string) (int64, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return 0, filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotFile)
	}
	object, err := m.client.StatObject(context.Background(), m.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return 0, filesystem.NewUnableToCheckExistence(path, err)
	}
	return object.Size, nil
}

func (m *MinioFileSystem) MimeType(path string) (string, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return "", filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotFile)
	}
	object, err := m.client.StatObject(context.Background(), m.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return "", filesystem.NewUnableToCheckExistence(path, err)
	}
	if object.ContentType == "" || object.ContentType == "application/octet-stream" {
		return m.mimeTypeDetector.DetectFromPath(path), nil
	}
	return object.ContentType, nil
}

func (m *MinioFileSystem) Visibility(path string) (string, error) {
	resp, err := m.client.GetObjectACL(context.Background(), m.bucket, path)
	if err != nil {
		return "", filesystem.NewUnableToCheckExistence(path, err)
	}
	return resp.Metadata.Get("x-amz-acl"), nil
}

func (m *MinioFileSystem) Write(path string, content []byte, config map[string]any) error {
	return m.WriteStream(path, bytes.NewReader(content), config)
}

func (m *MinioFileSystem) WriteStream(path string, stream io.Reader, _ map[string]any) error {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToWriteFile(path, filesystem.ErrIsNotFile)
	}
	var size int64 = -1
	if sizer, ok := stream.(interface{ Size() int64 }); ok {
		size = sizer.Size()
	}
	_, err := m.client.PutObject(context.Background(), m.bucket, path, stream, size, minio.PutObjectOptions{})
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	return nil
}

func (m *MinioFileSystem) SetVisibility(_ string, _ string) error {
	return exception.NewUnsupportedException("not supported yet")
}

func (m *MinioFileSystem) Delete(path string) error {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotFile)
	}
	err := m.client.RemoveObject(context.Background(), m.bucket, path, minio.RemoveObjectOptions{})
	if err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	return nil
}

func (m *MinioFileSystem) DeleteDir(path string) error {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToCheckExistence(path, filesystem.ErrIsNotDirectory)
	}
	err := m.client.RemoveObject(context.Background(), m.bucket, path, minio.RemoveObjectOptions{})
	if err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	return nil
}

func (m *MinioFileSystem) CreateDir(path string, _ map[string]any) error {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToCreateDirectory(path, filesystem.ErrIsNotFile)
	}
	_, err := m.client.PutObject(context.Background(), m.bucket, path, strings.NewReader(""), -1, minio.PutObjectOptions{})
	if err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	return nil
}

func (m *MinioFileSystem) Move(src string, dst string, _ map[string]any) error {
	src = filepath.ToSlash(src)
	dst = filepath.ToSlash(dst)
	if strings.HasSuffix(src, "/") {
		return filesystem.NewUnableToCheckExistence(src, filesystem.ErrIsNotFile)
	}
	if strings.HasSuffix(dst, "/") {
		return filesystem.NewUnableToCheckExistence(dst, filesystem.ErrIsNotFile)
	}
	_, err := m.client.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: m.bucket,
		Object: dst,
	}, minio.CopySrcOptions{
		Bucket: m.bucket,
		Object: src,
	})
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	err = m.client.RemoveObject(context.Background(), m.bucket, src, minio.RemoveObjectOptions{})
	if err != nil {
		return filesystem.NewUnableToDeleteFile(src, err)
	}
	return nil
}

func (m *MinioFileSystem) Copy(src string, dst string, _ map[string]any) error {
	src = filepath.ToSlash(src)
	dst = filepath.ToSlash(dst)
	if strings.HasSuffix(src, "/") {
		return filesystem.NewUnableToCheckExistence(src, filesystem.ErrIsNotFile)
	}
	if strings.HasSuffix(dst, "/") {
		return filesystem.NewUnableToCheckExistence(dst, filesystem.ErrIsNotFile)
	}
	_, err := m.client.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: m.bucket,
		Object: dst,
	}, minio.CopySrcOptions{
		Bucket: m.bucket,
		Object: src,
	})
	if err != nil {
		return filesystem.NewUnableToCopyFile(src, dst, err)
	}
	return nil
}
