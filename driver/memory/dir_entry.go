package memory

import (
	"io"
	"io/fs"
	"sort"
	"sync"
	"time"
)

const publicDirMode = 0755
const privateDirMode = 0700
const publicFileMode = 0644
const privateFileMode = 0600

type dirEntry struct {
	mu         *sync.RWMutex
	name       string
	isDir      bool
	entries    map[string]*dirEntry
	parent     *dirEntry
	content    []byte
	size       int64
	visibility string
	lastModify time.Time
}

func newDir(name string, visibility string, parent *dirEntry) *dirEntry {
	return &dirEntry{
		mu:         new(sync.RWMutex),
		name:       name,
		isDir:      true,
		entries:    make(map[string]*dirEntry),
		parent:     parent,
		visibility: visibility,
		lastModify: time.Now(),
	}
}

func newFile(name string, visibility string, parent *dirEntry) *dirEntry {
	return &dirEntry{
		mu:         new(sync.RWMutex),
		name:       name,
		isDir:      false,
		entries:    make(map[string]*dirEntry, 0),
		parent:     parent,
		visibility: visibility,
		lastModify: time.Now(),
	}
}

func (d *dirEntry) Name() string {
	return d.name
}

func (d *dirEntry) IsDir() bool {
	return d.isDir
}

func (d *dirEntry) Type() fs.FileMode {
	if d.isDir {
		return fs.ModeDir
	}
	return 0
}

func (d *dirEntry) Info() (fs.FileInfo, error) {
	return d, nil
}

func (d *dirEntry) Size() int64 {
	return d.size
}

func (d *dirEntry) Mode() fs.FileMode {
	return 0
}

func (d *dirEntry) ModTime() time.Time {
	return d.lastModify
}

func (d *dirEntry) Sys() any {
	return nil
}

func (d *dirEntry) writeStream(r io.Reader, appendMode bool) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if appendMode {
		d.content = append(d.content, content...)
	} else {
		d.content = content
	}
	d.lastModify = time.Now()
	return nil
}

func (d *dirEntry) findEntry(name string) *dirEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.entries[name]
}

func (d *dirEntry) addEntry(entry *dirEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry.parent = d
	d.entries[entry.name] = entry
}

func (d *dirEntry) deleteEntry(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.entries, name)
}

func (d *dirEntry) createDir(name string, visibility string) *dirEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := newDir(name, visibility, d)
	d.entries[name] = entry
	return entry
}

func (d *dirEntry) createFile(name string, visibility string) *dirEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := newFile(name, visibility, d)
	d.entries[name] = entry
	return entry
}

func (d *dirEntry) read() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.content, nil
}

func (d *dirEntry) readDir() []*dirEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	entries := make([]*dirEntry, 0, len(d.entries))
	for _, entry := range d.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries
}

func (d *dirEntry) walkDir() []*dirEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var entries []*dirEntry
	for _, entry := range d.entries {
		if entry.isDir {
			entries = append(entries, entry.walkDir()...)
		} else {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (d *dirEntry) copyTo(dst *dirEntry) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.isDir {
		return
	}
	dst.content = d.content
	dst.size = d.size
	dst.lastModify = d.lastModify
}
