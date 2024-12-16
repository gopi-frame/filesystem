package sftp

import (
	"errors"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Client is an interface that combines the SSH and SFTP clients.
type Client interface {
	// SSHClient returns the underlying SSH client.
	SSHClient() *ssh.Client
	// SFTPClient returns the underlying SFTP client.
	SFTPClient() *sftp.Client
	// Close closes the underlying connections.
	Close() error
}

type conn struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func newConn(sshClient *ssh.Client, sftpClient *sftp.Client) Client {
	return &conn{
		sshClient:  sshClient,
		sftpClient: sftpClient,
	}
}

func (c *conn) SSHClient() *ssh.Client {
	return c.sshClient
}

func (c *conn) SFTPClient() *sftp.Client {
	return c.sftpClient
}

func (c *conn) Close() error {
	return errors.Join(c.sshClient.Close(), c.sftpClient.Close())
}
