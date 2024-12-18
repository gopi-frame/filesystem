package filesystem

import (
	"strings"

	"github.com/go-viper/mapstructure/v2"
)

const (
	DirVisibilityKey  = "dir_visibility"
	FileVisibilityKey = "file_visibility"
	FileWriteFlagKey  = "file_write_flag"
)

type Config struct {
	DirVisibility  *string
	FileVisibility *string
	FileWriteFlag  *int
	remain         map[string]any `mapstructure:",remain"`
}

func NewConfig(configMap map[string]any) (*Config, error) {
	var cfg Config
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           &cfg,
		MatchName: func(mapKey, fieldName string) bool {
			return strings.EqualFold(mapKey, fieldName) ||
				strings.EqualFold(fieldName, strings.NewReplacer("-", "", "_", "").Replace(mapKey))
		},
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToBasicTypeHookFunc(),
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

func (cfg *Config) Others() map[string]any {
	return cfg.remain
}

func (cfg *Config) Get(key string) (any, bool) {
	if cfg.remain == nil {
		return nil, false
	}
	v, ok := cfg.remain[key]
	return v, ok
}

func (cfg *Config) Set(key string, value any) {
	if cfg.remain == nil {
		cfg.remain = make(map[string]any)
	}
	cfg.remain[key] = value
}
