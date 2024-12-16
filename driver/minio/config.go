package minio

import (
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/gopi-frame/env"

	"github.com/go-viper/mapstructure/v2"

	md5simd "github.com/minio/md5-simd"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Endpoint           string
	Bucket             string
	Region             string
	AccessKeyID        string
	SecretAccessKey    string
	SessionToken       string
	SignerType         credentials.SignatureType
	UseSSL             bool
	Transport          http.RoundTripper
	BucketLookup       minio.BucketLookupType
	CustomRegionViaURL func(u url.URL) string
	TrailingHeaders    bool
	MaxRetries         int
	CustomMD5          func() md5simd.Hasher
	CustomSHA256       func() md5simd.Hasher
}

func (c *Config) Apply(fs *MinioFileSystem) error {
	fs.bucket = c.Bucket
	if c.SignerType != credentials.SignatureV4 && c.SignerType != credentials.SignatureV2 {
		c.SignerType = credentials.SignatureV4
	}
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds: credentials.NewStatic(
			c.AccessKeyID,
			c.SecretAccessKey,
			c.SessionToken,
			c.SignerType,
		),
		Secure:             c.UseSSL,
		Transport:          c.Transport,
		BucketLookup:       c.BucketLookup,
		CustomRegionViaURL: c.CustomRegionViaURL,
		TrailingHeaders:    c.TrailingHeaders,
		MaxRetries:         c.MaxRetries,
		CustomMD5:          c.CustomMD5,
		CustomSHA256:       c.CustomSHA256,
	})
	if err != nil {
		return err
	}
	fs.client = client
	return nil
}

func ConfigFromMap(m map[string]any) (*Config, error) {
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
			func(from, to reflect.Type, data any) (any, error) {
				if from.Kind() == reflect.String && to == reflect.TypeFor[credentials.SignatureType]() {
					v, err := strconv.ParseUint(data.(string), 10, 8)
					if err != nil {
						return nil, err
					}
					return credentials.SignatureType(v), nil
				}
				return data, nil
			},
			func(from, to reflect.Type, data any) (any, error) {
				if from.Kind() == reflect.String && to == reflect.TypeFor[minio.BucketLookupType]() {
					v, err := strconv.ParseUint(data.(string), 10, 8)
					if err != nil {
						return nil, err
					}
					return minio.BucketLookupType(v), nil
				}
				return data, nil
			},
			mapstructure.StringToBasicTypeHookFunc(),
		),
	})
	if err != nil {
		return nil, err
	}
	err = decoder.Decode(m)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
