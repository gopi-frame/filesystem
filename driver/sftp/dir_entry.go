package sftp

import (
	"io/fs"
)

type dirEntry struct {
	fi fs.FileInfo
}

func (d *dirEntry) Name() string {
	return d.fi.Name()
}

func (d *dirEntry) IsDir() bool {
	return d.fi.IsDir()
}

func (d *dirEntry) Type() fs.FileMode {
	return d.fi.Mode().Type()
}

func (d *dirEntry) Info() (fs.FileInfo, error) {
	return d.fi, nil
}
