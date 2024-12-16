package local

import (
	"github.com/gopi-frame/filesystem"

	fs "github.com/gopi-frame/contract/filesystem"
)

// This variable can be modified by through -ldflags -X "github.com/gopi-frame/filesystem/adapter/local.driverName=custom"
var driverName = "local"

func init() {
	//goland:noinspection GoBoolExpressions
	if driverName != "" {
		filesystem.Register(driverName, &Driver{})
	}
}

type Driver struct{}

func (a *Driver) Open(options map[string]any) (fs.FileSystem, error) {
	cfg, err := ConfigFromMap(options)
	if err != nil {
		return nil, err
	}
	return NewLocalFileSystem(cfg.Root, cfg)
}
