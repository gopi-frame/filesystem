package sftp

import (
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

var driverName = "sftp"

func init() {
	//goland:noinspection GoBoolExpressions
	if driverName != "" {
		filesystem.Register(driverName, &Driver{})
	}
}

type Driver struct{}

func (d *Driver) Open(options map[string]any) (fs.FileSystem, error) {
	config, err := ConfigFromMap(options)
	if err != nil {
		return nil, err
	}
	return NewSFTPFileSystem(config)
}
