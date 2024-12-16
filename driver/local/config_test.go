package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileSystemConfigFromMap(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		var configMap = map[string]any{
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
		}
		config, err := ConfigFromMap(configMap)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "testdata", config.Root)
		assert.Equal(t, true, config.DeferRootCreation)
		assert.NotNil(t, config.Permissions)
		assert.NotNil(t, config.Permissions.File)
		assert.Equal(t, uint32(0644), config.Permissions.File.Public)
		assert.Equal(t, uint32(0600), config.Permissions.File.Private)
		assert.NotNil(t, config.Permissions.Directory)
		assert.Equal(t, uint32(0755), config.Permissions.Directory.Public)
		assert.Equal(t, uint32(0700), config.Permissions.Directory.Private)
		assert.NotNil(t, config.Visibility)
		assert.Equal(t, "public", config.Visibility.File)
		assert.Equal(t, "private", config.Visibility.Directory)
	})

	t.Run("partial", func(t *testing.T) {
		var configMap = map[string]any{
			"root":                "testdata",
			"defer_root_creation": true,
			"visibility": map[string]string{
				"file": "public",
			},
			"permissions": map[string]any{
				"file": map[string]any{
					"public": 0644,
				},
			},
		}
		config, err := ConfigFromMap(configMap)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		assert.Equal(t, "testdata", config.Root)
		assert.Equal(t, true, config.DeferRootCreation)
		assert.NotNil(t, config.Permissions)
		assert.NotNil(t, config.Permissions.File)
		assert.Equal(t, uint32(0644), config.Permissions.File.Public)
		assert.Nil(t, config.Permissions.File.Private)
		assert.Nil(t, config.Permissions.Directory)
		assert.NotNil(t, config.Visibility)
		assert.Equal(t, "public", config.Visibility.File)
	})
}
