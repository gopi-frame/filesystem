package memory

import (
	"strings"

	"github.com/gopi-frame/env"

	"github.com/go-viper/mapstructure/v2"
)

type Config struct {
	Visibility string
}

func ConfigFromMap(options map[string]any) (*Config, error) {
	var cfg Config
	if options == nil {
		return &cfg, nil
	}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           &cfg,
		MatchName: func(mapKey, fieldName string) bool {
			return strings.EqualFold(mapKey, fieldName) ||
				strings.EqualFold(fieldName, strings.NewReplacer("-", "", "_", "").Replace(mapKey))
		},
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			env.ExpandStringWithEnvHookFunc(),
			mapstructure.StringToBasicTypeHookFunc(),
			mapstructure.TextUnmarshallerHookFunc(),
		),
	})
	if err != nil {
		return nil, err
	}
	if err := decoder.Decode(options); err != nil {
		return nil, err
	}
	return &cfg, nil
}
