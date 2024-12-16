package ftp

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gopi-frame/filesystem/visibility/unix"

	"github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/ftp"

	"github.com/gopi-frame/env"

	"github.com/go-viper/mapstructure/v2"
)

type AuthConfig struct {
	Username string
	Password string
}

type DialerConfig struct {
	Timeout   *time.Duration
	KeepAlive *time.Duration
}

type TLSConfig struct {
	Explicit bool
	CertFile string
	KeyFile  string
}

type Config struct {
	Context           context.Context
	Debug             bool
	Addr              string
	Auth              *AuthConfig
	ShutdownTimeout   *time.Duration
	Dialer            *DialerConfig
	DisableEPSV       bool
	DisableUTF8       bool
	DisableMLSD       bool
	EnableWritingMDTM bool
	ForceListHidden   bool
	Loc               *time.Location
	TLS               *TLSConfig
	DialFunc          func(network, address string) (net.Conn, error)
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
	Visibility *struct {
		File      string
		Directory string
	}
	MimeTypeDetector filesystem.MimeTypeDetector
}

func ConfigFromMap(config map[string]any) (*Config, error) {
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
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToTimeHookFunc("2006-01-02 15:04:05"),
			func(from, to reflect.Type, data any) (any, error) {
				if from.Kind() == reflect.String && to == reflect.TypeFor[*time.Location]() {
					loc, err := time.LoadLocation(data.(string))
					if err != nil {
						return nil, err
					}
					return loc, nil
				}
				return data, nil
			},
			mapstructure.StringToBasicTypeHookFunc(),
			mapstructure.TextUnmarshallerHookFunc(),
		),
	})
	if err != nil {
		return nil, err
	}
	err = decoder.Decode(config)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) contextWithTimeout() (context.Context, context.CancelFunc) {
	var ctx context.Context
	if c.Context != nil {
		ctx = c.Context
	} else {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		if c.ShutdownTimeout != nil {
			ctx, cancel = context.WithTimeout(ctx, *c.ShutdownTimeout)
		} else {
			ctx, cancel = context.WithTimeout(ctx, ftp.DefaultDialTimeout)
		}
	}
	return ctx, cancel
}

func (c *Config) dialer() net.Dialer {
	dialer := net.Dialer{}
	if c.Dialer != nil {
		if c.Dialer.Timeout != nil {
			dialer.Timeout = *c.Dialer.Timeout
		}
		if c.Dialer.KeepAlive != nil {
			dialer.KeepAlive = *c.Dialer.KeepAlive
		}
	}
	return dialer
}

func (c *Config) tlsConfig() *tls.Config {
	if c.TLS == nil {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
	if err != nil {
		panic(err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return tlsConfig
}

func (c *Config) dialOptions() []ftp.DialOption {
	var opts []ftp.DialOption
	if c.Context != nil {
		opts = append(opts, ftp.DialWithContext(c.Context))
	}
	if c.ShutdownTimeout != nil {
		opts = append(opts, ftp.DialWithTimeout(*c.ShutdownTimeout))
	}
	if c.Dialer != nil {
		opts = append(opts, ftp.DialWithDialer(c.dialer()))
	}
	if c.TLS != nil {
		if c.TLS.Explicit {
			opts = append(opts, ftp.DialWithExplicitTLS(c.tlsConfig()))
		} else {
			opts = append(opts, ftp.DialWithTLS(c.tlsConfig()))
		}
	}
	if c.DisableEPSV {
		opts = append(opts, ftp.DialWithDisabledEPSV(true))
	}
	if c.DisableUTF8 {
		opts = append(opts, ftp.DialWithDisabledUTF8(true))
	}
	if c.DisableMLSD {
		opts = append(opts, ftp.DialWithDisabledMLSD(true))
	}
	if c.EnableWritingMDTM {
		opts = append(opts, ftp.DialWithWritingMDTM(true))
	}
	if c.ForceListHidden {
		opts = append(opts, ftp.DialWithForceListHidden(true))
	}
	if c.Loc != nil {
		opts = append(opts, ftp.DialWithLocation(c.Loc))
	}
	if c.Debug {
		opts = append(opts, ftp.DialWithDebugOutput(os.Stderr))
	}
	return opts
}

func (c *Config) Apply(f *FTPFileSystem) error {
	var m = make(map[string]string)
	if filePublic := c.Permissions.File.Public; filePublic > 0 {
		m["file_public"] = strconv.FormatUint(uint64(filePublic), 8)
	}
	if filePrivate := c.Permissions.File.Private; filePrivate > 0 {
		m["file_private"] = strconv.FormatUint(uint64(filePrivate), 8)
	}
	if dirPublic := c.Permissions.Directory.Public; dirPublic > 0 {
		m["dir_public"] = strconv.FormatUint(uint64(dirPublic), 8)
	}
	if dirPrivate := c.Permissions.Directory.Private; dirPrivate > 0 {
		m["dir_private"] = strconv.FormatUint(uint64(dirPrivate), 8)
	}
	if defaultFileVisibility := c.Visibility.File; defaultFileVisibility != "" {
		m["default_file_visibility"] = defaultFileVisibility
	}
	if defaultDirectoryVisibility := c.Visibility.Directory; defaultDirectoryVisibility != "" {
		m["default_directory_visibility"] = defaultDirectoryVisibility
	}
	if len(m) == 0 {
		return nil
	}
	f.visibilityConvertor = unix.NewFromMap(m)
	return nil
}
