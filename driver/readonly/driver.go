package readonly

import (
	fs "github.com/gopi-frame/contract/filesystem"
	"github.com/gopi-frame/filesystem"
)

var driverName = "readonly"

func init() {
	//goland:noinspection GoBoolExpressions
	if driverName != "" {
		filesystem.Register(driverName, &Driver{})
	}
}

type Driver struct{}

func (d *Driver) Open(options map[string]any) (fs.FileSystem, error) {
	return NewReadOnlyFileSystem(options["filesystem"].(fs.FileSystem)), nil
}
