package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDriver_Open(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		driver := &Driver{}
		fs, err := driver.Open(map[string]any{
			"root":                "testdata",
			"defer_root_creation": true,
			"visibility": map[string]string{
				"file":      "public",
				"directory": "private",
			},
			"permissions": map[string]any{
				"file": map[string]any{
					"public":  0644,
					"private": 0600,
				},
				"directory": map[string]any{
					"public":  "0755",
					"private": "0700",
				},
			},
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
		if err := fs.Delete("test.txt"); err != nil {
			assert.FailNow(t, err.Error())
		}
	})

	t.Run("partial", func(t *testing.T) {
		driver := &Driver{}
		fs, err := driver.Open(map[string]any{
			"root":                "testdata",
			"defer_root_creation": true,
		})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		if err := fs.Write("test.txt", []byte("hello"), nil); err != nil {
			assert.FailNow(t, err.Error())
		}
		content, err := fs.Read("test.txt")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "hello", string(content))
		if err := fs.Delete("test.txt"); err != nil {
			assert.FailNow(t, err.Error())
		}
	})
}
