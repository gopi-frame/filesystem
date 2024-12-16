package filesystem

import (
	"github.com/gopi-frame/collection/kv"
	"github.com/gopi-frame/contract/filesystem"
)

var drivers = kv.NewMap[string, filesystem.Driver]()

func Register(name string, driver filesystem.Driver) {
	drivers.Lock()
	defer drivers.Unlock()
	if drivers.ContainsKey(name) {
		panic(NewDuplicateDriver(name))
	}
	drivers.Set(name, driver)
}

func Drivers() []string {
	drivers.RLock()
	defer drivers.RUnlock()
	return drivers.Keys()
}

func Open(driverName string, options map[string]any) (filesystem.FileSystem, error) {
	return nil, nil
}
