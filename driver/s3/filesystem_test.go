package s3

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gopi-frame/filesystem"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var mockFS *S3FileSystem

func TestMain(m *testing.M) {
	fs, err := NewS3FileSystem(&Config{
		Bucket:   "mock",
		Endpoint: "http://localhost:19090/",
		Credentials: &Credentials{
			AccessKeyID:     "mock-access-key-id",
			SecretAccessKey: "mock-secret-access-key",
		},
		Options: []func(o *s3.Options){
			func(o *s3.Options) {
				o.UsePathStyle = true
			},
		},
	})
	if err != nil {
		panic(err)
	}
	mockFS = fs
	var files []string
	if err := filepath.WalkDir("./testdata/file", func(path string, d os.DirEntry, err error) error {
		fp := filepath.ToSlash(filepath.Clean(path))
		if d.IsDir() {
			fp += "/"
		}
		files = append(files, fp)
		return nil
	}); err != nil {
		panic(err)
	}
	for _, file := range files {
		var input *s3.PutObjectInput
		if strings.HasSuffix(file, "/") {
			input = &s3.PutObjectInput{
				Bucket: aws.String("mock"),
				Key:    aws.String(file),
			}
		} else {
			f, err := os.Open(file)
			if err != nil {
				panic(err)
			}
			defer func() {
				err = f.Close()
				if err != nil {
					fmt.Println("close file err: " + err.Error())
				}
			}()
			input = &s3.PutObjectInput{
				Bucket: aws.String("mock"),
				Key:    aws.String(file),
				Body:   f,
			}
		}
		_, err = fs.client.PutObject(context.Background(), input)
		if err != nil {
			panic(err)
		}
	}
	defer func() {
		objects, err := fs.client.ListObjects(context.Background(), &s3.ListObjectsInput{
			Bucket: aws.String("mock"),
			Prefix: aws.String("testdata/file/"),
		})
		if err != nil {
			panic(err)
		}
		var objectIdentifiers = make([]types.ObjectIdentifier, len(objects.Contents))
		for i, object := range objects.Contents {
			objectIdentifiers[i] = types.ObjectIdentifier{
				Key: object.Key,
			}
		}
		_, err = fs.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String("mock"),
			Delete: &types.Delete{
				Objects: objectIdentifiers,
			},
		})
		if err != nil {
			panic(err)
		}
	}()
	m.Run()
}

func TestS3FileSystem_Exists(t *testing.T) {
	t.Run("existing-empty-dir", func(t *testing.T) {
		exists, err := mockFS.Exists("testdata/file/for-read/empty/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("existing-non-empty-dir", func(t *testing.T) {
		exists, err := mockFS.Exists("testdata/file/for-read/non-empty/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("non-existing-dir", func(t *testing.T) {
		exists, err := mockFS.Exists("testdata/file/for-read/non-existing/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("existing-file", func(t *testing.T) {
		exists, err := mockFS.Exists("testdata/file/for-read/non-empty/empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
}

func TestS3FileSystem_DirExists(t *testing.T) {
	t.Run("existing-empty-dir", func(t *testing.T) {
		exists, err := mockFS.DirExists("testdata/file/for-read/empty/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("existing-non-empty-dir", func(t *testing.T) {
		exists, err := mockFS.DirExists("testdata/file/for-read/non-empty/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("non-existing-dir", func(t *testing.T) {
		exists, err := mockFS.DirExists("testdata/file/for-read/non-existing/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("existing-file", func(t *testing.T) {
		exists, err := mockFS.DirExists("testdata/file/for-read/non-empty/empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestS3FileSystem_FileExists(t *testing.T) {
	t.Run("existing-dir", func(t *testing.T) {
		exists, err := mockFS.FileExists("testdata/file/for-read/empty")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("existing-file", func(t *testing.T) {
		exists, err := mockFS.FileExists("testdata/file/for-read/non-empty/empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("non-existing-file", func(t *testing.T) {
		exists, err := mockFS.FileExists("testdata/file/for-read/non-existing.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestS3FileSystem_Read(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		content, err := mockFS.Read("testdata/file/for-read/non-empty/non-empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "non-empty", string(content))
	})
	t.Run("non-existing-file", func(t *testing.T) {
		_, err := mockFS.Read("testdata/file/for-read/non-empty/non-existing.txt")
		var respErr *http.ResponseError
		assert.ErrorAs(t, err, &respErr)
		assert.Equal(t, 404, respErr.HTTPStatusCode())
	})
	t.Run("existing-dir", func(t *testing.T) {
		_, err := mockFS.Read("testdata/file/for-read/non-empty/")
		assert.Error(t, err, filesystem.ErrIsNotFile)
	})
}

func TestS3FileSystem_ReadDir(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		_, err := mockFS.ReadDir("testdata/file/for-read/non-empty/non-empty.txt")
		assert.ErrorIs(t, err, filesystem.ErrIsNotDirectory)
	})
	t.Run("existing-dir", func(t *testing.T) {
		entries, err := mockFS.ReadDir("testdata/file/for-read/non-empty/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Len(t, entries, 5)
	})
	t.Run("non-existing-dir", func(t *testing.T) {
		entries, err := mockFS.ReadDir("testdata/file/for-read/non-existing/")
		assert.Len(t, entries, 0)
		assert.ErrorIs(t, err, nil)
	})
}

func TestS3FileSystem_WalkDir(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		err := mockFS.WalkDir("testdata/file/for-read/non-empty/non-empty.txt", func(path string, d fs.DirEntry, err error) error {
			return nil
		})
		assert.ErrorIs(t, err, filesystem.ErrIsNotDirectory)
	})
	t.Run("existing-dir", func(t *testing.T) {
		var entries []fs.DirEntry
		err := mockFS.WalkDir("testdata/file/for-read/non-empty/", func(path string, d fs.DirEntry, err error) error {
			entries = append(entries, d)
			return nil
		})
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Len(t, entries, 6)
	})
	t.Run("existing-dir-skip-dir", func(t *testing.T) {
		var entries []fs.DirEntry
		err := mockFS.WalkDir("testdata/file/for-read/non-empty/", func(path string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				return fs.SkipDir
			}
			entries = append(entries, d)
			return nil
		})
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Len(t, entries, 3)
	})
	t.Run("existing-dir-with-error", func(t *testing.T) {
		err := mockFS.WalkDir("testdata/file/for-read/non-empty/", func(path string, d fs.DirEntry, err error) error {
			return errors.New("test error")
		})
		assert.ErrorContains(t, err, "test error")
	})
	t.Run("non-existing-dir", func(t *testing.T) {
		var entries []fs.DirEntry
		err := mockFS.WalkDir("testdata/file/for-read/non-existing/", func(path string, d fs.DirEntry, err error) error {
			entries = append(entries, d)
			return nil
		})
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Len(t, entries, 0)
	})
}

func TestS3FileSystem_LastModified(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		_, err := mockFS.LastModified("testdata/file/for-read/non-empty/non-empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
	})
	t.Run("existing-dir", func(t *testing.T) {
		_, err := mockFS.LastModified("testdata/file/for-read/non-empty/")
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
	t.Run("non-existing-key", func(t *testing.T) {
		_, err := mockFS.LastModified("testdata/file/for-read/non-existing")
		var respErr *http.ResponseError
		assert.ErrorAs(t, err, &respErr)
		assert.Equal(t, 404, respErr.HTTPStatusCode())
	})
}

func TestS3FileSystem_FileSize(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		_, err := mockFS.FileSize("testdata/file/for-read/non-empty/non-empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
	})
	t.Run("existing-dir", func(t *testing.T) {
		_, err := mockFS.FileSize("testdata/file/for-read/non-empty/")
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
	t.Run("non-existing-key", func(t *testing.T) {
		_, err := mockFS.FileSize("testdata/file/for-read/non-existing")
		var respErr *http.ResponseError
		assert.ErrorAs(t, err, &respErr)
		assert.Equal(t, 404, respErr.HTTPStatusCode())
	})
}

func TestS3FileSystem_MimeType(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		mimeType, err := mockFS.MimeType("testdata/file/for-read/non-empty/non-empty.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "text/plain; charset=utf-8", mimeType)
	})
	t.Run("existing-dir", func(t *testing.T) {
		_, err := mockFS.MimeType("testdata/file/for-read/non-empty/")
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
	t.Run("non-existing-key", func(t *testing.T) {
		_, err := mockFS.MimeType("testdata/file/for-read/non-existing")
		var respErr *http.ResponseError
		assert.ErrorAs(t, err, &respErr)
		assert.Equal(t, 404, respErr.HTTPStatusCode())
	})
}

func TestS3FileSystem_Write(t *testing.T) {
	t.Run("non-existing-file", func(t *testing.T) {
		err := mockFS.Write("testdata/file/for-write/non-existing/non-existing.txt", []byte("test"), filesystem.PublicFile)
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		content, err := mockFS.Read("testdata/file/for-write/non-existing/non-existing.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test", string(content))
	})
	t.Run("existing-file-with-overwrite", func(t *testing.T) {
		content, err := mockFS.Read("testdata/file/for-write/create-file-but-existing-file-to-overwrite.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello world", string(content))
		err = mockFS.Write("testdata/file/for-write/create-file-but-existing-file-to-overwrite.txt", []byte("test"), filesystem.PublicFile)
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		content, err = mockFS.Read("testdata/file/for-write/create-file-but-existing-file-to-overwrite.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test", string(content))
	})
	t.Run("existing-file-with-append", func(t *testing.T) {
		content, err := mockFS.Read("testdata/file/for-write/create-file-but-existing-file-to-append.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
		err = mockFS.Write("testdata/file/for-write/create-file-but-existing-file-to-append.txt", []byte(" world"), filesystem.PublicFile.WithWriteFlag(os.O_APPEND))
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		content, err = mockFS.Read("testdata/file/for-write/create-file-but-existing-file-to-append.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello world", string(content))
	})
}

func TestS3FileSystem_Delete(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		err := mockFS.Delete("testdata/file/for-delete/file.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		exists, err := mockFS.FileExists("testdata/file/for-delete/file.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("existing-dir", func(t *testing.T) {
		err := mockFS.Delete("testdata/file/for-delete/delete-file-but-existing-dir/")
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
	t.Run("non-existing-file", func(t *testing.T) {
		err := mockFS.Delete("testdata/file/for-delete/non-existing.txt")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
	})
}

func TestS3FileSystem_DeleteDir(t *testing.T) {
	t.Run("existing-file", func(t *testing.T) {
		err := mockFS.DeleteDir("testdata/file/for-delete/file.txt")
		assert.ErrorIs(t, err, filesystem.ErrIsNotDirectory)
	})
	t.Run("existing-dir", func(t *testing.T) {
		err := mockFS.DeleteDir("testdata/file/for-delete/dir/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		exists, err := mockFS.DirExists("testdata/file/for-delete/dir/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("non-existing-dir", func(t *testing.T) {
		err := mockFS.DeleteDir("testdata/file/for-delete/non-existing/")
		if !assert.NoError(t, err) {
			assert.FailNow(t, err.Error())
		}
	})
}
