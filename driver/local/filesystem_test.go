package local

import (
	"os"
	"testing"

	"github.com/gopi-frame/filesystem"

	"github.com/stretchr/testify/assert"
)

var mockRoot = "./testdata"

var mockFS *LocalFileSystem

func TestMain(m *testing.M) {
	fs, err := NewLocalFileSystem(mockRoot)
	if err != nil {
		panic(err)
	}
	mockFS = fs
	m.Run()
}

func TestLocalFileSystem_Write(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		if err := mockFS.Write("test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := mockFS.Read("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
	})
	t.Run("with config", func(t *testing.T) {
		if err := mockFS.Write("test.txt", []byte(" world"), filesystem.PublicFile.WithWriteFlag(os.O_APPEND|os.O_RDWR|os.O_CREATE)); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := mockFS.Read("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello world", string(content))
	})
	if err := mockFS.Delete("test.txt"); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestLocalFileSystem_FileExists(t *testing.T) {
	exists, err := mockFS.FileExists("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.False(t, exists)

	if err := mockFS.Write("test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	exists, err = mockFS.FileExists("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.True(t, exists)

	exists, err = mockFS.FileExists(".")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.False(t, exists)
	if err := mockFS.Delete("test.txt"); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestLocalFileSystem_DirExists(t *testing.T) {
	exists, err := mockFS.DirExists("test")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.False(t, exists)

	exists, err = mockFS.DirExists(".")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.True(t, exists)
}

func TestLocalFileSystem_Exists(t *testing.T) {
	exists, err := mockFS.Exists("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.False(t, exists)

	if err := mockFS.Write("test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	exists, err = mockFS.Exists("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.True(t, exists)
	exists, err = mockFS.Exists(".")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.True(t, exists)
	if err := mockFS.Delete("test.txt"); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestLocalFileSystem_Move(t *testing.T) {
	if err := mockFS.Write("test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := mockFS.Move("test.txt", "test2.txt", nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	exists, err := mockFS.FileExists("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.False(t, exists)
	content, err := mockFS.Read("test2.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, "hello", string(content))
	if err := mockFS.Delete("test2.txt"); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestLocalFileSystem_Copy(t *testing.T) {
	if err := mockFS.Write("test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := mockFS.Copy("test.txt", "test2.txt", nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	content, err := mockFS.Read("test2.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, "hello", string(content))
	if err := mockFS.Delete("test.txt"); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := mockFS.Delete("test2.txt"); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestLocalFileSystem_CreateDir(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		if err := mockFS.CreateDir("test", nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := mockFS.DirExists("test")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
	})

	t.Run("with config", func(t *testing.T) {
		if err := mockFS.CreateDir("test2", filesystem.PrivateDir); err != nil {
			assert.FailNow(t, err.Error())
		}
		exists, err := mockFS.DirExists("test2")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.True(t, exists)
		v, err := mockFS.Visibility("test2")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "private", v)
	})
	if err := mockFS.DeleteDir("test"); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err := mockFS.DeleteDir("test2"); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestLocalFileSystem_SetVisibility(t *testing.T) {
	if err := mockFS.Write("test.txt", []byte("hello"), nil); err != nil {
		assert.FailNow(t, err.Error())
	}
	v, err := mockFS.Visibility("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, "public", v)
	if err := mockFS.SetVisibility("test.txt", "private"); err != nil {
		assert.FailNow(t, err.Error())
	}
	v, err = mockFS.Visibility("test.txt")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, "private", v)
}
