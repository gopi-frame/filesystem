package ftp

import (
	"github.com/gopi-frame/ftp"
)

type ConnPool interface {
	Get() (*ftp.ServerConn, error)
	Put(conn *ftp.ServerConn)
}

type connPool struct {
	config *Config
}

func newConnPool(config *Config) *connPool {
	return &connPool{
		config: config,
	}
}

func (c *connPool) Get() (*ftp.ServerConn, error) {
	conn, err := ftp.Dial(c.config.Addr, c.config.dialOptions()...)
	if err != nil {
		return nil, err
	}
	if c.config.Auth != nil {
		err = conn.Login(c.config.Auth.Username, c.config.Auth.Password)
		if err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (c *connPool) Put(conn *ftp.ServerConn) {
	_ = conn.Quit()
	return
}
