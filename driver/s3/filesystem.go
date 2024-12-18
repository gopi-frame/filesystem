package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gopi-frame/filesystem/visibility/acl"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	fs2 "github.com/gopi-frame/contract/filesystem"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/gopi-frame/filesystem"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3FileSystem struct {
	client *s3.Client

	bucket            string
	visibilityConvert acl.VisibilityConvertor
	mimeTypeDetector  fs2.MimeTypeDetector
}

func NewS3FileSystem(opts ...Option) (*S3FileSystem, error) {
	f := new(S3FileSystem)
	f.visibilityConvert = acl.New()
	f.mimeTypeDetector = filesystem.NewMimeTypeDetector()
	for _, opt := range opts {
		if err := opt.Apply(f); err != nil {
			return nil, err
		}
	}
	if f.client == nil {
		return nil, errors.New("s3 client is required")
	}
	return f, nil
}

// Exists checks if the given path exists.
// If the path ends with a slash, it checks if the directory exists.
// Otherwise, it checks if the file exists.
func (s *S3FileSystem) Exists(path string) (bool, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return s.DirExists(path)
	}
	return s.FileExists(path)
}

// FileExists checks if the given path exists and is a file.
// If the path ends with a slash, it returns false.
func (s *S3FileSystem) FileExists(path string) (bool, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return false, nil
	}
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		var respErr *awshttp.ResponseError
		if errors.As(err, &respErr) && respErr.Response.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return true, nil
}

// DirExists checks if the given path exists and is a directory.
// If the path does not end with a slash, it returns false.
func (s *S3FileSystem) DirExists(path string) (bool, error) {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return false, nil
	}
	resp, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(strings.TrimRight(path, "/") + "/"),
		MaxKeys:   aws.Int32(1),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		fmt.Println(err.Error())
		if errors.Is(err, (*types.NoSuchKey)(nil)) {
			return false, nil
		}
		return false, filesystem.NewUnableToCheckExistence(path, err)
	}
	return *resp.KeyCount > 0, nil
}

// Read reads the file at the given path and returns its contents.
// If the path ends with a slash, it returns an error.
func (s *S3FileSystem) Read(path string) ([]byte, error) {
	stream, err := s.ReadStream(path)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	return io.ReadAll(stream)
}

// ReadStream reads the file at the given path and returns a stream of its contents.
// If the path ends with a slash, it returns an error.
func (s *S3FileSystem) ReadStream(path string) (io.ReadCloser, error) {
	if strings.HasSuffix(filepath.ToSlash(path), "/") {
		return nil, filesystem.NewUnableToReadDirectory(path, filesystem.ErrIsNotFile)
	}
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, filesystem.NewUnableToReadFile(path, err)
	}
	return resp.Body, nil
}

// ReadDir reads the directory at the given path and returns a list of its contents.
// If the path does not end with a slash, it returns an error.
func (s *S3FileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return nil, filesystem.NewUnableToReadDirectory(path, filesystem.ErrIsNotDirectory)
	}
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(path),
	})
	var entries []os.DirEntry
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, filesystem.NewUnableToReadDirectory(path, err)
		}
		for _, obj := range page.Contents {
			if *obj.Key == path {
				continue
			}
			dirEntry := &dirEntry{
				obj: obj,
			}
			entries = append(entries, dirEntry)
		}
	}
	return entries, nil
}

// WalkDir walks the directory tree rooted at the given path, calling walkFn for each file or directory in the tree.
// If the path does not end with a slash, it returns an error.
func (s *S3FileSystem) WalkDir(path string, walkFn fs.WalkDirFunc) error {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToReadDirectory(path, filesystem.ErrIsNotDirectory)
	}
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(path),
	})
	var err error
	for paginator.HasMorePages() {
		var page *s3.ListObjectsV2Output
		page, err = paginator.NextPage(context.Background())
		if err != nil {
			return filesystem.NewUnableToReadDirectory(path, err)
		}
		for _, obj := range page.Contents {
			dirEntry := &dirEntry{
				obj: obj,
			}
			err = walkFn(*obj.Key, dirEntry, err)
			if err != nil {
				if errors.Is(err, filepath.SkipDir) || errors.Is(err, fs.SkipAll) {
					err = nil
				}
			}
		}
	}
	if err != nil && !errors.Is(err, fs.SkipDir) && !errors.Is(err, fs.SkipAll) {
		return filesystem.NewUnableToReadDirectory(path, err)
	}
	return nil
}

// LastModified returns the last modified time of the file at the given path.
// If the path ends with a slash, it returns an error.
func (s *S3FileSystem) LastModified(path string) (time.Time, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, filesystem.ErrIsNotFile)
	}
	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return time.Time{}, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	return *resp.LastModified, nil
}

// FileSize returns the size of the file at the given path.
// If the path ends with a slash, it returns an error.
func (s *S3FileSystem) FileSize(path string) (int64, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, filesystem.ErrIsNotFile)
	}
	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return 0, filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	return *resp.ContentLength, nil
}

// MimeType returns the mime type of the file at the given path.
// If the path ends with a slash, it returns an error.
func (s *S3FileSystem) MimeType(path string) (string, error) {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return "", filesystem.NewUnableToRetrieveMetadata(path, filesystem.ErrIsNotFile)
	}
	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return "application/octet-stream", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	if *resp.ContentType == "application/octet-stream" {
		return s.mimeTypeDetector.DetectFromPath(path), nil
	}
	return *resp.ContentType, nil
}

// Visibility returns the visibility of the object at the given path.
func (s *S3FileSystem) Visibility(path string) (string, error) {
	path = filepath.ToSlash(path)
	resp, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return "", filesystem.NewUnableToRetrieveMetadata(path, err)
	}
	owner := acl.Owner{ID: *resp.Owner.ID}
	grants := make([]acl.Grant, 0)
	for _, grant := range resp.Grants {
		grants = append(grants, acl.Grant{
			Permission: string(grant.Permission),
			Grantee: struct {
				ID  string
				URI string
			}{ID: *grant.Grantee.ID, URI: *grant.Grantee.URI},
		})
	}
	return s.visibilityConvert.ACLToVisibility(owner, grants), nil
}

// Write writes the given content to the file at the given path.
// If the path ends with a slash, it returns an error.
// Default it will try to create the file if it does not exist.
// If the file already exists, it will overwrite it.
// If the file already exists and you want to append to it,
// make sure the write flags returned by config.WriteFlag() contains os.O_APPEND
func (s *S3FileSystem) Write(path string, content []byte, config map[string]any) error {
	return s.WriteStream(path, bytes.NewReader(content), config)
}

// WriteStream writes the given content to the file at the given path.
// If the path ends with a slash, it returns an error.
// Default it will try to create the file if it does not exist.
// If the file already exists, it will overwrite it.
// If the file already exists and you want to append to it,
// make sure the write flags returned by config.WriteFlag() contains os.O_APPEND
//
// Note about the append mode:
//
//	if the object's storage class is [types.StorageClassExpressOnezone],
//	it will use the s3.PutObjectInput.WriteOffsetBytes field to specify the offset to write to.
//	Else, it will read the content of the original file first and then append the new content to it.
func (s *S3FileSystem) WriteStream(path string, stream io.Reader, config map[string]any) error {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToWriteFile(path, filesystem.ErrIsNotFile)
	}
	var fileMode = s.visibilityConvert.DefaultForFile()
	var writeFlag int
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return filesystem.NewUnableToWriteFile(path, err)
		}
		if cfg.DirVisibility != nil {
			fileMode = *cfg.DirVisibility
		}
		if cfg.FileVisibility != nil {
			fileMode = *cfg.FileVisibility
		}
		if cfg.FileWriteFlag != nil {
			writeFlag = *cfg.FileWriteFlag
		}
	}
	var err error
	var input = &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		Body:   stream,
		ACL:    types.ObjectCannedACL(fileMode),
	}
	if writeFlag&os.O_APPEND > 0 {
		fi, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(path),
		})
		if err != nil {
			return filesystem.NewUnableToWriteFile(path, err)
		}
		if fi.StorageClass != types.StorageClassExpressOnezone {
			content, err := s.Read(path)
			if err != nil {
				return filesystem.NewUnableToWriteFile(path, err)
			}
			var stream2 = bytes.NewBuffer(nil)
			if _, err := stream2.Write(content); err != nil {
				return filesystem.NewUnableToWriteFile(path, err)
			}
			if _, err := stream2.ReadFrom(stream); err != nil {
				return filesystem.NewUnableToWriteFile(path, err)
			}
			input.Body = bytes.NewReader(stream2.Bytes())
		} else {
			input.WriteOffsetBytes = fi.ContentLength
		}
	}
	_, err = s.client.PutObject(context.Background(), input)
	if err != nil {
		return filesystem.NewUnableToWriteFile(path, err)
	}
	return nil
}

// SetVisibility sets the visibility of the file at the given path.
func (s *S3FileSystem) SetVisibility(path string, visibility string) error {
	path = filepath.ToSlash(path)
	fileACL := visibility
	_, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		ACL:    types.ObjectCannedACL(fileACL),
	})
	if err != nil {
		return filesystem.NewUnableToSetPermission(path, err)
	}
	return nil
}

// Delete deletes the file at the given path.
// If the path ends with a slash, it returns an error.
func (s *S3FileSystem) Delete(path string) error {
	path = filepath.ToSlash(path)
	if strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToDeleteFile(path, filesystem.ErrIsNotFile)
	}
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return filesystem.NewUnableToDeleteFile(path, err)
	}
	return nil
}

// DeleteDir deletes the directory at the given path.
// If the path does not end with a slash, it returns an error.
func (s *S3FileSystem) DeleteDir(path string) error {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToDeleteDirectory(path, filesystem.ErrIsNotDirectory)
	}
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(path),
	})
	var err error
	for paginator.HasMorePages() {
		var page *s3.ListObjectsV2Output
		page, err = paginator.NextPage(context.Background())
		if err != nil {
			return filesystem.NewUnableToDeleteDirectory(path, err)
		}
		for _, obj := range page.Contents {
			_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return filesystem.NewUnableToDeleteDirectory(path, err)
			}
		}
	}
	return nil
}

// CreateDir creates a directory at the given path.
// If the path does not end with a slash, it returns an error.
func (s *S3FileSystem) CreateDir(path string, config map[string]any) error {
	path = filepath.ToSlash(path)
	if !strings.HasSuffix(path, "/") {
		return filesystem.NewUnableToCreateDirectory(path, filesystem.ErrIsNotDirectory)
	}
	exists, _ := s.DirExists(path)
	if exists {
		return nil
	}
	var dirMode = s.visibilityConvert.DefaultForDir()
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return filesystem.NewUnableToCreateDirectory(path, err)
		}
		if cfg.DirVisibility != nil {
			dirMode = *cfg.DirVisibility
		}
	}
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimRight(path, "/") + "/"),
		ACL:    types.ObjectCannedACL(dirMode),
	})
	if err != nil {
		return filesystem.NewUnableToCreateDirectory(path, err)
	}
	return nil
}

// Move moves the object at the given path to the given destination.
// This operation only supports moving files.
func (s *S3FileSystem) Move(src string, dst string, config map[string]any) error {
	if strings.HasSuffix(src, "/") {
		return filesystem.NewUnableToMove(src, dst, filesystem.ErrIsNotFile)
	}
	if !strings.HasSuffix(dst, "/") {
		return filesystem.NewUnableToMove(src, dst, filesystem.ErrIsNotDirectory)
	}
	var dirMode = s.visibilityConvert.DefaultForDir()
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return filesystem.NewUnableToMove(src, dst, err)
		}
		if cfg.DirVisibility != nil {
			dirMode = *cfg.DirVisibility
		}
	}
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(src),
		Key:        aws.String(dst),
		ACL:        types.ObjectCannedACL(dirMode),
	})
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(src),
	})
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	return nil
}

// Copy copies the object at the given path to the given destination.
// If any one of the source and destination ends with a slash, it returns an error.
func (s *S3FileSystem) Copy(src string, dst string, config map[string]any) error {
	src = filepath.ToSlash(src)
	dst = filepath.ToSlash(dst)
	if strings.HasSuffix(src, "/") || strings.HasSuffix(dst, "/") {
		return filesystem.NewUnableToCopyFile(src, dst, filesystem.ErrIsNotFile)
	}
	var dirMode = s.visibilityConvert.DefaultForDir()
	if config != nil {
		cfg, err := filesystem.NewConfig(config)
		if err != nil {
			return filesystem.NewUnableToCopyFile(src, dst, err)
		}
		if cfg.DirVisibility != nil {
			dirMode = *cfg.DirVisibility
		}
	}
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(src),
		Key:        aws.String(dst),
		ACL:        types.ObjectCannedACL(dirMode),
	})
	if err != nil {
		return filesystem.NewUnableToMove(src, dst, err)
	}
	return nil
}
