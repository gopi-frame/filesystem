package filesystem

var PublicFile = NewCreateFileConfig("public", "public")
var PrivateFile = NewCreateFileConfig("private", "private")
var PublicFileWithPrivateDir = NewCreateFileConfig("private", "public")
var PrivateFileWithPublicDir = NewCreateFileConfig("public", "private")

type CreateFileConfig struct {
	CreateDirectoryConfig
	writeFlag      int
	fileVisibility string
}

func NewCreateFileConfig(dirVisibility, fileVisibility string) *CreateFileConfig {
	return &CreateFileConfig{
		CreateDirectoryConfig: *NewCreateDirectoryConfig(dirVisibility),
		fileVisibility:        fileVisibility,
	}
}

func (c *CreateFileConfig) WithWriteFlag(writeFlag int) *CreateFileConfig {
	newCfg := &CreateFileConfig{
		CreateDirectoryConfig: c.CreateDirectoryConfig,
		fileVisibility:        c.fileVisibility,
		writeFlag:             writeFlag,
	}
	return newCfg
}

func (c *CreateFileConfig) WriteFlag() int {
	return c.writeFlag
}

func (c *CreateFileConfig) FileVisibility() string {
	return c.fileVisibility
}
