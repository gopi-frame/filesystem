package s3

import (
	"context"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/gopi-frame/env"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type SharedConfig struct {
	Profile          string
	ConfigFiles      []string
	CredentialsFiles []string
}

type Config struct {
	// Name is the name of the s3 client.
	Name string
	// Endpoint is the endpoint to connect to.
	Endpoint string
	// Bucket is the name of the S3 bucket to connect to. This value is required and cannot be empty.
	Bucket string
	// Region is the AWS region to connect to.
	Region string
	// Credentials are the credentials to use to connect to the S3 API.
	Credentials *Credentials
	// SharedConfig is the shared configuration to use to connect to the S3 API.
	SharedConfig *SharedConfig
	// ConfigOptions are the extra options to use to configure the S3 SDK.
	ConfigOptions []func(*config.LoadOptions) error
	// Options are the extra options to use to configure the S3 client.
	Options []func(o *s3.Options)
}

func (c *Config) Apply(fs *S3FileSystem) error {
	cfg, err := c.awsConfig()
	if err != nil {
		return err
	}
	fs.client = s3.NewFromConfig(cfg, func(options *s3.Options) {
		for _, opt := range c.Options {
			opt(options)
		}
	})
	fs.bucket = c.Bucket
	return nil
}

func (c *Config) configOptions() []func(*config.LoadOptions) error {
	var options []func(*config.LoadOptions) error
	if c.Region != "" {
		options = append(options, config.WithRegion(c.Region))
	} else {
		options = append(options, config.WithRegion("auto"))
	}
	if c.Credentials != nil {
		options = append(options, config.WithCredentialsProvider(
			aws.NewCredentialsCache(
				credentials.NewStaticCredentialsProvider(
					c.Credentials.AccessKeyID,
					c.Credentials.SecretAccessKey,
					c.Credentials.SessionToken,
				),
			),
		))
	}
	if c.SharedConfig != nil {
		if c.SharedConfig.Profile != "" {
			options = append(options, config.WithSharedConfigProfile(c.SharedConfig.Profile))
		}
		if len(c.SharedConfig.ConfigFiles) > 0 {
			options = append(options, config.WithSharedConfigFiles(c.SharedConfig.ConfigFiles))
		}
		if len(c.SharedConfig.CredentialsFiles) > 0 {
			options = append(options, config.WithSharedCredentialsFiles(c.SharedConfig.CredentialsFiles))
		}
	}
	if c.Endpoint != "" {
		options = append(options, config.WithBaseEndpoint(c.Endpoint))
	}
	for _, option := range c.ConfigOptions {
		options = append(options, option)
	}
	return options
}

func (c *Config) awsConfig() (aws.Config, error) {
	return config.LoadDefaultConfig(context.Background(), c.configOptions()...)
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
