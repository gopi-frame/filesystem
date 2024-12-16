package sftp

import (
	"bufio"
	"errors"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/pkg/sftp"

	"github.com/gopi-frame/env"

	"github.com/go-viper/mapstructure/v2"

	"golang.org/x/crypto/ssh/agent"

	"golang.org/x/crypto/ssh"
)

type Config struct {
	// Host is the host to connect to.
	Host string
	// Port is the port to connect to.
	Port int
	// Username is the username to use.
	Username string
	// Password is the password to use.
	Password string
	// SSHAuthSock is the path to the SSH authentication agent socket.
	SSHAuthSock string
	// PrivateKey is the path to the private key to use.
	// Only one of PrivateKey and PrivateKeyPath should be set.
	PrivateKey string
	// PrivateKeyPath is the path to the private key to use.
	// Only one of PrivateKey and PrivateKeyPath should be set.
	PrivateKeyPath string
	// PrivateKeyPassphrase is the password to use for the private key.
	PrivateKeyPassphrase string
	// IgnoreHostKey is true if the host key should be ignored.
	IgnoreHostKey bool
	// HostKey is the public key to use.
	HostKey string
	// KnownHosts is the path to the known_hosts file.
	// It is only used when HostKey is empty.
	// If empty, the default known_hosts file is used.
	//  - On Non Windows systems, it is $HOME/.ssh/known_hosts.
	//  - On Windows systems, it is %USERPROFILE%/.ssh/known_hosts.
	KnownHosts string

	MaxPacket                    int
	MaxConcurrentRequestsPerFile int
	UseConcurrentReads           bool
	UseConcurrentWrites          bool
	UseFstat                     bool

	once      sync.Once
	sshConfig *ssh.ClientConfig
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
	sshClientConfig, err := cfg.sshClientConfig()
	if err != nil {
		return nil, err
	}
	cfg.sshConfig = sshClientConfig
	return &cfg, nil
}

func (c *Config) sshClientConfig() (*ssh.ClientConfig, error) {
	clientConfig := &ssh.ClientConfig{
		User: c.Username,
	}
	var auths []ssh.AuthMethod
	var privateKeyParser = func(key []byte) (ssh.Signer, error) {
		if c.PrivateKeyPassphrase != "" {
			return ssh.ParsePrivateKeyWithPassphrase(key, []byte(c.PrivateKeyPassphrase))
		}
		return ssh.ParsePrivateKey(key)
	}
	if c.PrivateKey != "" {
		signer, err := privateKeyParser([]byte(c.PrivateKey))
		if err != nil {
			return nil, err
		}
		auths = append(auths, ssh.PublicKeys(signer))
	} else if c.PrivateKeyPath != "" {
		key, err := os.ReadFile(c.PrivateKeyPath)
		if err != nil {
			return nil, err
		}
		signer, err := privateKeyParser(key)
		if err != nil {
			return nil, err
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}
	if c.SSHAuthSock != "" {
		if conn, err := net.Dial("unix", c.SSHAuthSock); err == nil {
			auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
		} else {
			return nil, err
		}
	}
	if c.Password != "" {
		auths = append(auths, ssh.Password(c.Password))
	}
	clientConfig.Auth = auths
	hostKeyCallback, err := c.getHostKeyCallback()
	if err != nil {
		return nil, err
	}
	clientConfig.HostKeyCallback = hostKeyCallback
	return clientConfig, nil
}

func (c *Config) getHostKeyCallback() (ssh.HostKeyCallback, error) {
	if c.IgnoreHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	if c.HostKey != "" {
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(c.HostKey))
		if err != nil {
			return nil, err
		}
		return ssh.FixedHostKey(pub), nil
	}
	var knownHosts string
	if c.KnownHosts != "" {
		knownHosts = c.KnownHosts
	} else {
		knownHosts = knownHostsFile
	}
	f, err := os.Open(knownHosts)
	if err != nil {
		panic(errors.New("unable to read known_hosts file at " + knownHosts))
	}
	defer func() {
		_ = f.Close()
	}()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		if len(fields) != 3 {
			continue
		}
		if strings.Contains(fields[0], c.Host) {
			pub, _, _, _, err := ssh.ParseAuthorizedKey(scanner.Bytes())
			if err != nil {
				return nil, err
			}
			return ssh.FixedHostKey(pub), nil
		}
	}
	return nil, errors.New("unable to find host in known_hosts file")
}

func (c *Config) sftpOptions() []sftp.ClientOption {
	var opts []sftp.ClientOption
	if c.MaxPacket > 0 {
		opts = append(opts, sftp.MaxPacket(c.MaxPacket))
	}
	if c.MaxConcurrentRequestsPerFile > 0 {
		opts = append(opts, sftp.MaxConcurrentRequestsPerFile(c.MaxConcurrentRequestsPerFile))
	}
	if c.UseConcurrentWrites {
		opts = append(opts, sftp.UseConcurrentWrites(c.UseConcurrentWrites))
	}
	if c.UseFstat {
		opts = append(opts, sftp.UseFstat(c.UseFstat))
	}
	if c.UseConcurrentReads {
		opts = append(opts, sftp.UseConcurrentReads(c.UseConcurrentReads))
	}
	return opts
}
