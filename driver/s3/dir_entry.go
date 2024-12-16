package s3

import (
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type dirEntry struct {
	obj types.Object
}

func (d *dirEntry) Size() int64 {
	return *d.obj.Size
}

func (d *dirEntry) Mode() fs.FileMode {
	return fs.FileMode(0)
}

func (d *dirEntry) ModTime() time.Time {
	return *d.obj.LastModified
}

func (d *dirEntry) Sys() any {
	return d.obj
}

func (d *dirEntry) Name() string {
	return filepath.Base(*d.obj.Key)
}

func (d *dirEntry) IsDir() bool {
	return strings.HasSuffix(*d.obj.Key, "/")
}

func (d *dirEntry) Type() fs.FileMode {
	if d.IsDir() {
		return fs.ModeDir
	}
	return fs.FileMode(0)
}

func (d *dirEntry) Info() (fs.FileInfo, error) {
	return d, nil
}
