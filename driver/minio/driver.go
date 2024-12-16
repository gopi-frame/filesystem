package minio

import (
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

var driverName = "minio"

func init() {
	//goland:noinspection GoBoolExpressions
	if driverName != "" {
		filesystem.Register(driverName, &Driver{})
	}
}

type Driver struct{}

func (d *Driver) Open(options map[string]any) (fs.FileSystem, error) {
	cfg, err := ConfigFromMap(options)
	if err != nil {
		return nil, err
	}
	return NewMinioFileSystem(cfg)
}
