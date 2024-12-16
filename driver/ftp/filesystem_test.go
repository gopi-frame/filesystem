package ftp

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gopi-frame/filesystem"

	"github.com/stretchr/testify/assert"
)

var testRoot = filepath.Join("testdata", "ftp", "admin")

func TestMain(m *testing.M) {
	if err := prepareForTest(); err != nil {
		panic(err)
	}
	defer func() {
		if err := os.RemoveAll("testdata"); err != nil {
			panic(err)
		}
	}()
	m.Run()
}

func prepareForTest() (err error) {
	// for read tests
	err = os.MkdirAll(filepath.Join(testRoot, "for-read"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-read", "dir1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-read", "dir1", "dir2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-read", "file1.txt"), []byte("file1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-read", "file2.txt"), []byte("file2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-read", "dir1", "file1.txt"), []byte("file1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-read", "dir1", "dir2", "file1.txt"), []byte("file1"), os.ModePerm)
	if err != nil {
		return
	}
	// for write tests
	err = os.MkdirAll(filepath.Join(testRoot, "for-write", "dir1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-write", "dir2", "dir3"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-write", "dir1", "file1.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-write", "dir2", "dir3", "file2.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-write", "dir2", "dir3", "file1.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	// for delete tests
	err = os.MkdirAll(filepath.Join(testRoot, "for-delete", "dir1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-delete", "dir2", "dir3"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-delete", "empty"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-delete", "dir1", "file1.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-delete", "dir2", "dir3", "file2.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-delete", "file1.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-delete", "file2.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	// for move tests
	err = os.MkdirAll(filepath.Join(testRoot, "for-move-src", "dir1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-move-src", "dir2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-move-src", "file1.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-move-src", "file2.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-move-dst", "dir2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-move-dst", "file2.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	// for copy tests
	err = os.MkdirAll(filepath.Join(testRoot, "for-copy-src", "dir1"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-copy-src", "dir2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-copy-src", "file1.txt"), []byte("test"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-copy-src", "file2.txt"), []byte("src-test-2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-copy-src", "file3.txt"), []byte("src-test-3"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(testRoot, "for-copy-dst", "dir2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-copy-dst", "file2.txt"), []byte("dst-test-2"), os.ModePerm)
	if err != nil {
		return
	}
	err = os.WriteFile(filepath.Join(testRoot, "for-copy-dst", "file3.txt"), []byte("dst-test-3"), os.ModePerm)
	if err != nil {
		return
	}
	return
}

func forReadTest(path string) string {
	return filepath.ToSlash(filepath.Join("for-read", path))
}

func forWriteTest(path string) string {
	return filepath.ToSlash(filepath.Join("for-write", path))
}

func forDeleteTest(path string) string {
	return filepath.ToSlash(filepath.Join("for-delete", path))
}

func forMoveTest(path string, src bool) string {
	if src {
		return filepath.ToSlash(filepath.Join("for-move-src", path))
	}
	return filepath.ToSlash(filepath.Join("for-move-dst", path))
}

func forCopyTest(path string, src bool) string {
	if src {
		return filepath.ToSlash(filepath.Join("for-copy-src", path))
	}
	return filepath.ToSlash(filepath.Join("for-copy-dst", path))
}

func newfs(t *testing.T) *FTPFileSystem {
	fs, err := NewFTPFileSystem(&Config{
		Addr: "127.0.0.1:6021",
		Auth: &AuthConfig{Username: "admin", Password: "123456"},
	})
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return fs
}

func TestFTPFileSystem_Exists(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		exists, err := fs.Exists(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("not exists file", func(t *testing.T) {
		exists, err := fs.Exists(forReadTest("not-exists.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("exists dir", func(t *testing.T) {
		exists, err := fs.Exists(forReadTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("not exists dir", func(t *testing.T) {
		exists, err := fs.Exists("not-exists")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestFTPFileSystem_FileExists(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		exists, err := fs.FileExists(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("not exists file", func(t *testing.T) {
		exists, err := fs.FileExists(forReadTest("not-exists.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("exists dir", func(t *testing.T) {
		exists, err := fs.FileExists(forReadTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("not exists dir", func(t *testing.T) {
		exists, err := fs.FileExists(forReadTest("not-exists"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestFTPFileSystem_DirExists(t *testing.T) {
	fs := newfs(t)
	t.Run("exists dir", func(t *testing.T) {
		exists, err := fs.DirExists(forReadTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("not exists dir", func(t *testing.T) {
		exists, err := fs.DirExists(forReadTest("not-exists"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("exists file", func(t *testing.T) {
		exists, err := fs.DirExists(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("not exists file", func(t *testing.T) {
		exists, err := fs.DirExists(forReadTest("not-exists.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestFTPFileSystem_ReadDir(t *testing.T) {
	fs := newfs(t)
	t.Run("exists dir", func(t *testing.T) {
		entries, err := fs.ReadDir(forReadTest(""))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 3, len(entries))
	})
	t.Run("not exists dir", func(t *testing.T) {
		entries, err := fs.ReadDir(forReadTest("not-exists"))
		assert.ErrorIs(t, err, os.ErrNotExist)
		assert.Equal(t, 0, len(entries))
	})
	t.Run("exists file", func(t *testing.T) {
		entries, err := fs.ReadDir(forReadTest("file1.txt"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotDirectory)
		assert.Equal(t, 0, len(entries))
	})
}

func TestFTPFileSystem_WalkDir(t *testing.T) {
	fs := newfs(t)
	t.Run("exists dir", func(t *testing.T) {
		var entries []os.DirEntry
		err := fs.WalkDir(forReadTest("."), func(path string, info os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			entries = append(entries, info)
			return nil
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 6, len(entries))
	})
	t.Run("skip dir", func(t *testing.T) {
		var entries []os.DirEntry
		err := fs.WalkDir(forReadTest("."), func(path string, info os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
			entries = append(entries, info)
			return nil
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 4, len(entries))
	})
	t.Run("return error", func(t *testing.T) {
		err := fs.WalkDir(forReadTest("."), func(path string, info os.DirEntry, err error) error {
			return errors.New("test")
		})
		assert.ErrorContains(t, err, "test")
	})
	t.Run("not exists dir", func(t *testing.T) {
		var entries []os.DirEntry
		err := fs.WalkDir(forReadTest("not-exists"), func(path string, info os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			entries = append(entries, info)
			return nil
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 0, len(entries))
	})
}

func TestFTPFileSystem_Read(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		content, err := fs.Read(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "file1", string(content))
	})
	t.Run("not exists file", func(t *testing.T) {
		_, err := fs.Read(forReadTest("not-exists.txt"))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exists dir", func(t *testing.T) {
		_, err := fs.Read(forReadTest("dir1"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
}

func TestFTPFileSystem_LastModified(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		modifiedTime := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
		if err := os.Chtimes(filepath.Join(testRoot, "for-read", "file1.txt"), time.Time{}, modifiedTime); err != nil {
			assert.FailNow(t, err.Error())
		}
		mtime, err := fs.LastModified(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, modifiedTime, mtime)
	})
	t.Run("not exists file", func(t *testing.T) {
		_, err := fs.LastModified(forReadTest("not-exists.txt"))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exists dir", func(t *testing.T) {
		_, err := fs.LastModified(forReadTest("dir1"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
}

func TestFTPFileSystem_FileSize(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		size, err := fs.FileSize(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, int64(5), size)
	})
	t.Run("not exists file", func(t *testing.T) {
		_, err := fs.FileSize(forReadTest("not-exists.txt"))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exists dir", func(t *testing.T) {
		_, err := fs.FileSize(forReadTest("dir1"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
}

func TestFTPFileSystem_MimeType(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		mimeType, err := fs.MimeType(forReadTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "text/plain; charset=utf-8", mimeType)
	})
	t.Run("not exists file", func(t *testing.T) {
		_, err := fs.MimeType(forReadTest("not-exists.txt"))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exists dir", func(t *testing.T) {
		_, err := fs.MimeType(forReadTest("dir1"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
}

func TestFTPFileSystem_Write(t *testing.T) {
	fs := newfs(t)
	t.Run("create file", func(t *testing.T) {
		err := fs.Write(forWriteTest("create.txt"), []byte("test"), filesystem.PublicFile)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read(forWriteTest("create.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test", string(content))
	})
	t.Run("override file", func(t *testing.T) {
		err := fs.Write(forWriteTest("create.txt"), []byte("test2"), filesystem.PublicFile)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read(forWriteTest("create.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test2", string(content))
	})
	t.Run("append file", func(t *testing.T) {
		err := fs.Write(forWriteTest("create.txt"), []byte("test3"), filesystem.PublicFile.WithWriteFlag(os.O_APPEND))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read(forWriteTest("create.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test2test3", string(content))
	})
	t.Run("with dir path", func(t *testing.T) {
		err := fs.Write(forWriteTest("dir1/dir2/create.txt"), []byte("test"), filesystem.PublicFile)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read(forWriteTest("dir1/dir2/create.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test", string(content))
	})
}

func TestFTPFileSystem_Delete(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		exists, err := fs.Exists(forDeleteTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.Delete(forDeleteTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forDeleteTest("file1.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("not exists file", func(t *testing.T) {
		err := fs.Delete(forDeleteTest("not-exists.txt"))
		assert.Nil(t, err)
	})
	t.Run("exists dir", func(t *testing.T) {
		exists, err := fs.Exists(forDeleteTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.Delete(forDeleteTest("dir1"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
}

func TestFTPFileSystem_DeleteDir(t *testing.T) {
	fs := newfs(t)
	t.Run("exists empty dir", func(t *testing.T) {
		exists, err := fs.Exists(forDeleteTest("empty"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.DeleteDir(forDeleteTest("empty"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forDeleteTest("empty"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("non-empty dir", func(t *testing.T) {
		exists, err := fs.Exists(forDeleteTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.DeleteDir(forDeleteTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forDeleteTest("dir1"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("reverse", func(t *testing.T) {
		exists, err := fs.Exists(forDeleteTest("dir2"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.DeleteDir(forDeleteTest("dir2"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forDeleteTest("dir2"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
	t.Run("exists file", func(t *testing.T) {
		exists, err := fs.Exists(forDeleteTest("file2.txt"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.DeleteDir(forDeleteTest("file2.txt"))
		assert.ErrorIs(t, err, filesystem.ErrIsNotDirectory)
	})
}

func TestFTPFileSystem_CreateDir(t *testing.T) {
	fs := newfs(t)
	t.Run("create dir", func(t *testing.T) {
		err := fs.CreateDir(forWriteTest("create"), nil)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.Exists(forWriteTest("create"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("create dir with parent", func(t *testing.T) {
		err := fs.CreateDir(forWriteTest("create2/dir"), nil)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.Exists(forWriteTest("create2/dir"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("exists dir", func(t *testing.T) {
		err := fs.CreateDir(forWriteTest("dir1"), nil)
		assert.Nil(t, err)
	})
	t.Run("exists file", func(t *testing.T) {
		err := fs.CreateDir(forWriteTest("create.txt"), nil)
		assert.ErrorIs(t, err, filesystem.ErrIsNotDirectory)
	})
}

func TestFTPFileSystem_Move(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file", func(t *testing.T) {
		exists, err := fs.Exists(forMoveTest("file1.txt", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.Move(forMoveTest("file1.txt", true), forMoveTest("file1.txt", false), nil)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forMoveTest("file1.txt", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		content, err := fs.Read(forMoveTest("file1.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "test", string(content))
	})
	t.Run("not exists file", func(t *testing.T) {
		err := fs.Move(forMoveTest("not-exists.txt", true), forMoveTest("not-exists.txt", false), nil)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exists dir", func(t *testing.T) {
		exists, err := fs.Exists(forMoveTest("dir1", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.Move(forMoveTest("dir1", true), forMoveTest("dir1", false), nil)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forMoveTest("dir1", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		exists, err = fs.Exists(forMoveTest("dir1", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("not exists dir", func(t *testing.T) {
		err := fs.Move(forMoveTest("not-exists", true), forMoveTest("not-exists", false), nil)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exists target", func(t *testing.T) {
		err := fs.Move(forMoveTest("file2.txt", true), forMoveTest("file2.txt", false), nil)
		assert.ErrorIs(t, err, os.ErrExist)
	})
}

func TestFTPFileSystem_Copy(t *testing.T) {
	fs := newfs(t)
	t.Run("exists file to non-existing target", func(t *testing.T) {
		exists, err := fs.Exists(forCopyTest("file1.txt", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.Copy(forCopyTest("file1.txt", true), forCopyTest("file1.txt", false), nil)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forCopyTest("file1.txt", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.Exists(forCopyTest("file1.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("exists file to existing target", func(t *testing.T) {
		exists, err := fs.Exists(forCopyTest("file2.txt", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		err = fs.Copy(forCopyTest("file2.txt", true), forCopyTest("file2.txt", false), nil)
		assert.ErrorIs(t, err, os.ErrExist)
	})
	t.Run("exists file to existing target with overwrite", func(t *testing.T) {
		exists, err := fs.Exists(forCopyTest("file2.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		content, err := fs.Read(forCopyTest("file2.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "dst-test-2", string(content))
		err = fs.Copy(forCopyTest("file2.txt", true), forCopyTest("file2.txt", false), filesystem.PublicFile.WithWriteFlag(os.O_TRUNC))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists(forCopyTest("file2.txt", true))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.Exists(forCopyTest("file2.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		content, err = fs.Read(forCopyTest("file2.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "src-test-2", string(content))
	})
	t.Run("exists file to existing target with append", func(t *testing.T) {
		exists, err := fs.Exists(forCopyTest("file3.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		content, err := fs.Read(forCopyTest("file3.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "dst-test-3", string(content))
		err = fs.Copy(forCopyTest("file3.txt", true), forCopyTest("file3.txt", false), filesystem.PublicFile.WithWriteFlag(os.O_APPEND))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err = fs.Read(forCopyTest("file3.txt", false))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "dst-test-3src-test-3", string(content))
	})
}
