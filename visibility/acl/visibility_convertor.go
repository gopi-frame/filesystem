package acl

type Grant struct {
	Permission string
	Grantee    struct {
		ID  string
		URI string
	}
}

type Owner struct {
	ID string
}

// VisibilityConvertor is an interface that defines methods for converting between visibility strings and acl strings
type VisibilityConvertor interface {
	ACLToVisibility(owner Owner, grants []Grant) string
	DefaultForFile() string
	DefaultForDir() string
}

const PublicGranteeURI = "http://acs.amazonaws.com/groups/global/AllUsers"
const AuthenticatedGranteeURI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
const GrantsPermissionRead = "READ"
const GrantsPermissionWrite = "WRITE"
const PublicReadACL = "public-read"
const AuthenticatedRead = "authenticated-read"
const BucketOwnerRead = "bucket-owner-read"
const PublicReadWrite = "public-read-write"
const PrivateACL = "private"

type ACLVisibilityConvertor struct {
	defaultDirVisibility  string
	defaultFileVisibility string
}

// New returns a new ACLVisibilityConvertor instance.
// The default values are:
//   - default file visibility: public-read
//   - default directory visibility: public-read
//
// The default values can be changed by calling the Set* methods.
func New() *ACLVisibilityConvertor {
	return &ACLVisibilityConvertor{
		defaultFileVisibility: PublicReadACL,
		defaultDirVisibility:  PublicReadACL,
	}
}

// SetDefaultFileVisibility sets the default file visibility.
func (a *ACLVisibilityConvertor) SetDefaultFileVisibility(visibility string) {
	a.defaultFileVisibility = visibility
}

// SetDefaultDirVisibility sets the default directory visibility.
func (a *ACLVisibilityConvertor) SetDefaultDirVisibility(visibility string) {
	a.defaultDirVisibility = visibility
}

// ACLToVisibility converts ACL settings to visibility string
func (a *ACLVisibilityConvertor) ACLToVisibility(owner Owner, grants []Grant) string {
	switch {
	case len(grants) == 1:
		if grants[0].Grantee.URI == "" && grants[0].Permission == "FULL_CONTROL" {
			return PrivateACL
		}
	case len(grants) == 2:
		for _, grant := range grants {
			if grant.Grantee.URI == PublicGranteeURI && grant.Permission == GrantsPermissionRead {
				return PublicReadACL
			}
			if grant.Grantee.URI == AuthenticatedGranteeURI && grant.Permission == GrantsPermissionRead {
				return AuthenticatedRead
			}
			if grant.Permission == GrantsPermissionRead && owner.ID == grant.Grantee.ID {
				return BucketOwnerRead
			}
		}
	case len(grants) == 3:
		for _, grant := range grants {
			if grant.Grantee.URI == PublicGranteeURI && grant.Permission == GrantsPermissionWrite {
				return PublicReadWrite
			}
		}
	}
	return ""
}

// DefaultForFile returns the default visibility for file
func (a *ACLVisibilityConvertor) DefaultForFile() string {
	return a.defaultFileVisibility
}

// DefaultForDir returns the default visibility for directory
func (a *ACLVisibilityConvertor) DefaultForDir() string {
	return a.defaultDirVisibility
}
