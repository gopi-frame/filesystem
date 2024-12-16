package sftp

import (
	"fmt"
	"sync/atomic"

	"github.com/pkg/sftp"

	"golang.org/x/crypto/ssh"
)

// ClientPool is an interface that manages Client
type ClientPool interface {
	// Get returns a new Client instance
	Get() (Client, error)
	// Put puts a Client instance back into the pool.
	Put(Client)
}

type clientPool struct {
	client atomic.Value
	config *Config
}

func newClientPool(config *Config) ClientPool {
	return &clientPool{
		config: config,
	}
}

func (p *clientPool) Get() (Client, error) {
	if client := p.client.Load(); client != nil {
		_, err := client.(Client).SFTPClient().Getwd()
		if err == nil {
			return client.(Client), nil
		}
	}
	addr := fmt.Sprintf("%s:%d", p.config.Host, p.config.Port)
	sshClient, err := ssh.Dial("tcp", addr, p.config.sshConfig)
	if err != nil {
		return nil, err
	}
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, err
	}
	client := newConn(sshClient, sftpClient)
	p.client.Store(client)
	return client, nil
}

func (p *clientPool) Put(_ Client) {
}
