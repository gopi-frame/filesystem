package ftp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigFromMap(t *testing.T) {
	configMap := map[string]any{
		"addr": "127.0.0.1:21",
		"auth": map[string]any{
			"username": "foo",
			"password": "bar",
		},
		"dialer": map[string]any{
			"timeout": "10s",
		},
		"tls": map[string]any{
			"explicit": true,
			"certFile": "cert.pem",
			"keyFile":  "key.pem",
		},
	}
	config, err := ConfigFromMap(configMap)
	if assert.NoError(t, err) {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, "127.0.0.1:21", config.Addr)
	assert.Equal(t, "foo", config.Auth.Username)
	assert.Equal(t, "bar", config.Auth.Password)
	assert.Equal(t, "10s", config.Dialer.Timeout.String())
	assert.Equal(t, true, config.TLS.Explicit)
	assert.Equal(t, "cert.pem", config.TLS.CertFile)
	assert.Equal(t, "key.pem", config.TLS.KeyFile)
	assert.Equal(t, "10s", config.Dialer.Timeout.String())

}
