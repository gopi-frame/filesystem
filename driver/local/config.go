package local

import (
	"strconv"
	"strings"

	"github.com/gopi-frame/filesystem/visibility/unix"

	"github.com/go-viper/mapstructure/v2"
	"github.com/gopi-frame/env"

	fs "github.com/gopi-frame/contract/filesystem"
)

type Config struct {
	Root              string
	DeferRootCreation bool
	Permissions       struct {
		File struct {
			Public  uint32
			Private uint32
		}
		Directory struct {
			Public  uint32
			Private uint32
		}
	}
	Visibility struct {
		File      string
		Directory string
	}
	MimeTypeDetector fs.MimeTypeDetector
}

func (c *Config) Apply(f *LocalFileSystem) error {
	f.root = c.Root
	f.deferRootCreation = c.DeferRootCreation
	if c.MimeTypeDetector != nil {
		f.mimetypeDetector = c.MimeTypeDetector
	}
	m := make(map[string]string)
	if c.Permissions.File.Public != 0 {
		m["file_public"] = strconv.FormatUint(uint64(c.Permissions.File.Public), 8)
	}
	if c.Permissions.File.Private != 0 {
		m["file_private"] = strconv.FormatUint(uint64(c.Permissions.File.Private), 8)
	}
	if c.Permissions.Directory.Public != 0 {
		m["dir_public"] = strconv.FormatUint(uint64(c.Permissions.Directory.Public), 8)
	}
	if c.Permissions.Directory.Private != 0 {
		m["dir_private"] = strconv.FormatUint(uint64(c.Permissions.Directory.Private), 8)
	}
	if c.Visibility.File != "" {
		m["default_file_visibility"] = c.Visibility.File
	}
	if c.Visibility.Directory != "" {
		m["default_dir_visibility"] = c.Visibility.Directory
	}
	f.visibilityConvertor = unix.NewFromMap(m)
	return nil
}

func ConfigFromMap(configMap map[string]any) (*Config, error) {
	var cfg Config
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           &cfg,
		MatchName: func(mapKey, fieldName string) bool {
			return strings.EqualFold(mapKey, fieldName) ||
				strings.EqualFold(fieldName, strings.NewReplacer("-", "", "_", "").Replace(mapKey))
		},
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			env.ExpandStringWithEnvHookFunc(),
			env.ExpandStringKeyMapWithEnvHookFunc(),
			mapstructure.StringToBasicTypeHookFunc(),
			mapstructure.TextUnmarshallerHookFunc(),
		),
	})
	if err != nil {
		return nil, err
	}
	if err := decoder.Decode(configMap); err != nil {
		return nil, err
	}
	return &cfg, nil
}
