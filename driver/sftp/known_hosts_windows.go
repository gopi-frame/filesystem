//go:build windows

package sftp

import (
	"os"
	"path/filepath"
)

var (
	knownHostsFile = filepath.Join(os.Getenv("USERPROFILE"), ".ssh", "known_hosts")
)
