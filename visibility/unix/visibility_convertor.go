package unix

import (
	"os"
	"strconv"
)

// VisibilityConvertor is an interface that defines methods for converting between visibility strings and file modes.
type VisibilityConvertor interface {
	ForFile(visibility string) os.FileMode
	ForDir(visibility string) os.FileMode
	InverseForFile(mode os.FileMode) string
	InverseForDir(mode os.FileMode) string
	DefaultForFile() os.FileMode
	DefaultForDir() os.FileMode
}

// UnixVisibilityConvertor is a unix-style visibility converter.
// It provides methods to convert between unix-style permissions and visibility strings.
type UnixVisibilityConvertor struct {
	dirPublic   os.FileMode
	dirPrivate  os.FileMode
	filePublic  os.FileMode
	filePrivate os.FileMode

	dirDefaultVisibility  string
	fileDefaultVisibility string
}

// New returns a new UnixVisibilityConvertor instance.
// The default values are:
//   - public directory permission: 0755
//   - private directory permission: 0700
//   - public file permission: 0644
//   - private file permission: 0600
//   - default directory visibility: public
//   - default file visibility: public
//
// The default values can be changed by calling the Set* methods.
func New() *UnixVisibilityConvertor {
	return &UnixVisibilityConvertor{
		dirPublic:             os.FileMode(0755),
		dirPrivate:            os.FileMode(0700),
		filePublic:            os.FileMode(0644),
		filePrivate:           os.FileMode(0600),
		dirDefaultVisibility:  "public",
		fileDefaultVisibility: "public",
	}
}

// NewFromMap returns a new UnixVisibilityConvertor instance from a map.
// If the map is nil, a new instance with the default values is returned.
//
// The following keys are supported:
//   - dir_public: the public directory permission, e.g. 0755
//   - dir_private: the private directory permission, e.g. 0700
//   - file_public: the public file permission, e.g. 0644
//   - file_private: the private file permission, e.g. 0600
//   - dir_default_visibility: the default directory visibility, e.g. public
//   - file_default_visibility: the default file visibility, e.g. public
//
// If a key is not present in the map or can't be parsed, the default value is used.
func NewFromMap(m map[string]string) *UnixVisibilityConvertor {
	v := New()
	if m == nil {
		return v
	}
	if perm, err := strconv.ParseUint(m["dir_public"], 8, 32); err == nil {
		v.SetDirPublic(os.FileMode(perm))
	}
	if perm, err := strconv.ParseUint(m["dir_private"], 8, 32); err == nil {
		v.SetDirPrivate(os.FileMode(perm))
	}
	if perm, err := strconv.ParseUint(m["file_public"], 8, 32); err == nil {
		v.SetFilePublic(os.FileMode(perm))
	}
	if perm, err := strconv.ParseUint(m["file_private"], 8, 32); err == nil {
		v.SetFilePrivate(os.FileMode(perm))
	}
	if value, ok := m["dir_default_visibility"]; ok {
		v.SetDirDefaultVisibility(value)
	}
	if value, ok := m["file_default_visibility"]; ok {
		v.SetFileDefaultVisibility(value)
	}
	return v
}

// SetDirPublic sets the public directory permission.
func (u *UnixVisibilityConvertor) SetDirPublic(perm os.FileMode) {
	u.dirPublic = perm
}

// SetDirPrivate sets the private directory permission.
func (u *UnixVisibilityConvertor) SetDirPrivate(perm os.FileMode) {
	u.dirPrivate = perm
}

// SetFilePublic sets the public file permission.
func (u *UnixVisibilityConvertor) SetFilePublic(perm os.FileMode) {
	u.filePublic = perm
}

// SetFilePrivate sets the private file permission.
func (u *UnixVisibilityConvertor) SetFilePrivate(perm os.FileMode) {
	u.filePrivate = perm
}

// SetDirDefaultVisibility sets the default directory visibility.
// Only "public" and "private" are allowed, otherwise it will be ignored.
func (u *UnixVisibilityConvertor) SetDirDefaultVisibility(visibility string) {
	if visibility != "public" && visibility != "private" {
		return
	}
	u.dirDefaultVisibility = visibility
}

// SetFileDefaultVisibility sets the default file visibility.
// Only "public" and "private" are allowed, otherwise it will be ignored.
func (u *UnixVisibilityConvertor) SetFileDefaultVisibility(visibility string) {
	if visibility != "public" && visibility != "private" {
		return
	}
	u.fileDefaultVisibility = visibility
}

// ForFile returns the file mode for the given visibility.
// If the visibility is "public", the public file mode is returned.
// If the visibility is "private", the private file mode is returned.
// Otherwise, the default file mode is returned.
func (u *UnixVisibilityConvertor) ForFile(visibility string) os.FileMode {
	if visibility != "public" && visibility != "private" {
		visibility = u.fileDefaultVisibility
	}
	if visibility == "public" {
		return u.filePublic
	}
	return u.filePrivate
}

// ForDir returns the file mode for the given visibility.
// If the visibility is "public", the public file mode is returned.
// If the visibility is "private", the private file mode is returned.
// Otherwise, the default file mode is returned.
func (u *UnixVisibilityConvertor) ForDir(visibility string) os.FileMode {
	if visibility != "public" && visibility != "private" {
		visibility = u.dirDefaultVisibility
	}
	if visibility == "public" {
		return u.dirPublic
	}
	return u.dirPrivate
}

// InverseForFile returns the visibility for the given file mode.
// If the file mode is the public file mode, "public" is returned.
// If the file mode is the private file mode, "private" is returned.
// Otherwise, the default file visibility is returned.
func (u *UnixVisibilityConvertor) InverseForFile(mode os.FileMode) string {
	if mode == u.filePublic {
		return "public"
	}
	if mode == u.filePrivate {
		return "private"
	}
	return u.fileDefaultVisibility
}

// InverseForDir returns the visibility for the given file mode.
// If the file mode is the public file mode, "public" is returned.
// If the file mode is the private file mode, "private" is returned.
// Otherwise, the default file visibility is returned.
func (u *UnixVisibilityConvertor) InverseForDir(mode os.FileMode) string {
	if mode == u.dirPublic {
		return "public"
	}
	if mode == u.dirPrivate {
		return "private"
	}
	return u.dirDefaultVisibility
}

// DefaultForFile returns the default file mode.
func (u *UnixVisibilityConvertor) DefaultForFile() os.FileMode {
	if u.fileDefaultVisibility == "public" {
		return u.filePublic
	}
	return u.filePrivate
}

// DefaultForDir returns the default file mode.
func (u *UnixVisibilityConvertor) DefaultForDir() os.FileMode {
	if u.dirDefaultVisibility == "public" {
		return u.dirPublic
	}
	return u.dirPrivate
}
