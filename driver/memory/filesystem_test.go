package memory

import (
	"io"
	"os"
	"testing"

	"github.com/gopi-frame/filesystem"

	"github.com/stretchr/testify/assert"
)

func TestMemoryFileSystem_CreateDir(t *testing.T) {
	t.Run("mkdir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.DirExists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		if err := fs.DeleteDir("dir1"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.DirExists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})

	t.Run("mkdirAll", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1/dir2/dir3/dir4", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.DirExists("dir1/dir2/dir3/dir4")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		if err := fs.DeleteDir("dir1/dir2/dir3/dir4"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.DirExists("dir1/dir2/dir3/dir4")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
	})

	t.Run("mkdirAllWithDotEnd", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1/dir2/dir3/dir4/.", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.DirExists("dir1/dir2/dir3/dir4")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		if err := fs.DeleteDir("dir1/dir2/dir3/dir4"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.DirExists("dir1/dir2/dir3/dir4")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestMemoryFileSystem_Write(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
		v, err := fs.Visibility("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "public", v)
		if err := fs.Delete("test.txt"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})

	t.Run("writeWithConfig", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("test.txt", []byte("hello"), filesystem.PrivateFile.WithWriteFlag(os.O_APPEND)); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("test.txt", []byte(" world"), filesystem.PrivateFile.WithWriteFlag(os.O_APPEND)); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello world", string(content))
		v, err := fs.Visibility("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "private", v)
		if err := fs.Delete("test.txt"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})

	t.Run("writeWithMkdirAll", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/dir2/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read("dir1/dir2/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
		v, err := fs.Visibility("dir1/dir2/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "public", v)
		if err := fs.DeleteDir("dir1"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("dir1/dir2/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestMemoryFileSystem_Exists(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		exists, err := fs.Exists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		if err := fs.Write("test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.FileExists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.DirExists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		if err := fs.Delete("test.txt"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})

	t.Run("dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		exists, err := fs.Exists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		if err := fs.CreateDir("dir1", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.DirExists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.FileExists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		if err := fs.DeleteDir("dir1"); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err = fs.Exists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
	})
}

func TestMemoryFileSystem_ReadStream(t *testing.T) {
	t.Run("exists file", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		stream, err := fs.ReadStream("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := io.ReadAll(stream)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
	})

	t.Run("not exists file", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		_, err := fs.ReadStream("test.txt")
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		_, err := fs.ReadStream("dir1")
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestMemoryFileSystem_ReadDir(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		files, err := fs.ReadDir("")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 1, len(files))
	})

	t.Run("exists empty dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		files, err := fs.ReadDir("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 0, len(files))
	})

	t.Run("exists dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.CreateDir("dir1", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		files, err := fs.ReadDir("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 1, len(files))
	})
}

func TestMemoryFileSystem_WalkDir(t *testing.T) {
	fs := NewMemoryFileSystem("public", nil)
	if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := fs.Write("dir1/dir2/test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := fs.Write("dir0/dir3/dir4/test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	t.Run("root", func(t *testing.T) {
		var entries []string
		err := fs.WalkDir("", func(path string, info os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			entries = append(entries, path)
			return nil
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 3, len(entries))
		assert.Contains(t, entries, "dir1/test.txt")
		assert.Contains(t, entries, "dir1/dir2/test.txt")
		assert.Contains(t, entries, "dir0/dir3/dir4/test.txt")
	})

	t.Run("sub dir", func(t *testing.T) {
		var entries []string
		err := fs.WalkDir("dir1", func(path string, info os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			entries = append(entries, path)
			return nil
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, 2, len(entries))
		assert.Contains(t, entries, "dir1/test.txt")
		assert.Contains(t, entries, "dir1/dir2/test.txt")
	})
}

func TestMemoryFileSystem_Move(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir1/dir2/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir0/dir3/dir4/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Move("dir1/test.txt", "dir1/test2.txt", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("dir1/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		exists, err = fs.FileExists("dir1/test2.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir1/dir2/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir0/dir3/dir4/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Move("dir1", "dir11", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.DirExists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		exists, err = fs.DirExists("dir11")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("file to different dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir1/dir2/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir0/dir3/dir4/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Move("dir1/test.txt", "dir2/test2.txt", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("dir1/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		exists, err = fs.FileExists("dir2/test2.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("dir to different dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir1/dir2/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir0/dir3/dir4/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Move("dir1", "dir2/dir11", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.DirExists("dir1")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.False(t, exists)
		exists, err = fs.DirExists("dir2/dir11")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})
	t.Run("unknown source", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		err := fs.Move("dir1/test.txt", "dir2/test2.txt", nil)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exist dest", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir2/test2.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		err := fs.Move("dir1/test.txt", "dir2/test2.txt", nil)
		assert.ErrorIs(t, err, os.ErrExist)
	})
}

func TestMemoryFileSystem_Copy(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Copy("dir1/test.txt", "dir1/test2.txt", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("dir1/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.FileExists("dir1/test2.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		content, err := fs.Read("dir1/test2.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
	})
	t.Run("not exist source", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		err := fs.Copy("dir1/test.txt", "dir1/test2.txt", nil)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("exist dest non force", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir2/test2.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		err := fs.Copy("dir1/test.txt", "dir2/test2.txt", nil)
		assert.ErrorIs(t, err, os.ErrExist)
	})
	t.Run("exist dest force", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("dir2/test2.txt", []byte("hello2222"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Copy("dir1/test.txt", "dir2/test2.txt", filesystem.PublicFile.WithWriteFlag(os.O_TRUNC)); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := fs.FileExists("dir1/test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		exists, err = fs.FileExists("dir2/test2.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		content, err := fs.Read("dir2/test2.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
	})
	t.Run("dir", func(t *testing.T) {
		fs := NewMemoryFileSystem("public", nil)
		if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		err := fs.Copy("dir1", "dir2", nil)
		assert.ErrorIs(t, err, filesystem.ErrIsNotFile)
	})
}

func TestMemoryFileSystem_SetVisibility(t *testing.T) {
	fs := NewMemoryFileSystem("public", nil)
	if err := fs.Write("dir1/test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := fs.SetVisibility("dir1/test.txt", "private"); err != nil {
		assert.FailNow(t, err.Error())
	}
	v, err := fs.Visibility("dir1/test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, "private", v)
}
