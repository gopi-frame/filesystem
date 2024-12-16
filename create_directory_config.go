package filesystem

var PublicDir = NewCreateDirectoryConfig("public")
var PrivateDir = NewCreateDirectoryConfig("private")

type CreateDirectoryConfig struct {
	dirVisibility string
}

func NewCreateDirectoryConfig(dirVisibility string) *CreateDirectoryConfig {
	return &CreateDirectoryConfig{
		dirVisibility: dirVisibility,
	}
}

func (c *CreateDirectoryConfig) DirVisibility() string {
	return c.dirVisibility
}
