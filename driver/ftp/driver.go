package ftp

import (
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

var driverName = "ftp"

func init() {
	//goland:noinspection GoBoolExpressions
	if driverName != "" {
		filesystem.Register(driverName, &Driver{})
	}
}

type Driver struct {
}

func (d *Driver) Open(configMap map[string]any) (fs.FileSystem, error) {
	config, err := ConfigFromMap(configMap)
	if err != nil {
		return nil, err
	}
	return NewFTPFileSystem(config)
}
