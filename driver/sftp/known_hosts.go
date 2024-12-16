//go:build !windows

package sftp

import (
	"os"
	"path/filepath"
)

var knownHostsFile = filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts")
