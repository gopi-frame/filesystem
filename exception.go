package filesystem

import (
	"errors"
	"fmt"

	"github.com/gopi-frame/exception"

	. "github.com/gopi-frame/contract/exception"
)

var ErrIsNotDirectory = errors.New("entry is not a directory")
var ErrIsNotFile = errors.New("entry is not a file")

type DuplicateDriver struct {
	name string
	Throwable
}

func NewDuplicateDriver(name string) *DuplicateDriver {
	return &DuplicateDriver{
		name:      name,
		Throwable: exception.New(fmt.Sprintf("Duplicate driver: %s", name)),
	}
}

type UnableToCheckExistence struct {
	location string
	err      error
	Throwable
}

func NewUnableToCheckExistence(location string, err error) *UnableToCheckExistence {
	return &UnableToCheckExistence{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to check existence of file at location %s", location)),
	}
}

func (err *UnableToCheckExistence) Unwrap() error {
	return err.err
}

type UnableToReadFile struct {
	location string
	err      error
	Throwable
}

func NewUnableToReadFile(location string, err error) *UnableToReadFile {
	return &UnableToReadFile{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to read file at location %s: %s", location, err)),
	}
}

func (err *UnableToReadFile) Unwrap() error {
	return err.err
}

type UnableToCreateDirectory struct {
	location string
	err      error
	Throwable
}

func NewUnableToCreateDirectory(location string, err error) *UnableToCreateDirectory {
	return &UnableToCreateDirectory{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to create directory at location %s: %s", location, err)),
	}
}

func (err *UnableToCreateDirectory) Unwrap() error {
	return err.err
}

type UnableToRetrieveMetadata struct {
	location string
	err      error
	Throwable
}

func NewUnableToRetrieveMetadata(location string, err error) *UnableToRetrieveMetadata {
	return &UnableToRetrieveMetadata{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to retrieve metadata at location %s: %s", location, err)),
	}
}

func (err *UnableToRetrieveMetadata) Unwrap() error {
	return err.err
}

type UnableToWriteFile struct {
	location string
	err      error
	Throwable
}

func NewUnableToWriteFile(location string, err error) *UnableToWriteFile {
	return &UnableToWriteFile{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to write file at location %s: %s", location, err)),
	}
}

func (err *UnableToWriteFile) Unwrap() error {
	return err.err
}

type UnableToCloseFile struct {
	location string
	err      error
	Throwable
}

func NewUnableToCloseFile(location string, err error) *UnableToCloseFile {
	return &UnableToCloseFile{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to close file at location %s: %s", location, err)),
	}
}

func (err *UnableToCloseFile) Unwrap() error {
	return err.err
}

type UnableToSetPermission struct {
	location string
	err      error
	Throwable
}

func NewUnableToSetPermission(location string, err error) *UnableToSetPermission {
	return &UnableToSetPermission{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to set permission at location %s: %s", location, err)),
	}
}

func (err *UnableToSetPermission) Unwrap() error {
	return err.err
}

type UnableToDeleteFile struct {
	location string
	err      error
	Throwable
}

func NewUnableToDeleteFile(location string, err error) *UnableToDeleteFile {
	return &UnableToDeleteFile{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to delete file at location %s: %s", location, err)),
	}
}

func (err *UnableToDeleteFile) Unwrap() error {
	return err.err
}

type UnableToDeleteDirectory struct {
	location string
	err      error
	Throwable
}

func NewUnableToDeleteDirectory(location string, err error) *UnableToDeleteDirectory {
	return &UnableToDeleteDirectory{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to delete directory at location %s: %s", location, err)),
	}
}

func (err *UnableToDeleteDirectory) Unwrap() error {
	return err.err
}

type UnableToMove struct {
	src string
	dst string
	err error
	Throwable
}

func NewUnableToMove(src string, dst string, err error) *UnableToMove {
	return &UnableToMove{
		src:       src,
		dst:       dst,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to move file from %s to %s: %s", src, dst, err)),
	}
}

func (err *UnableToMove) Unwrap() error {
	return err.err
}

type UnableToCopyFile struct {
	src string
	dst string
	err error
	Throwable
}

func NewUnableToCopyFile(src string, dst string, err error) *UnableToCopyFile {
	return &UnableToCopyFile{
		src:       src,
		dst:       dst,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to copy from %s to %s: %s", src, dst, err)),
	}
}

func (err *UnableToCopyFile) Unwrap() error {
	return err.err
}

type UnableToReadDirectory struct {
	location string
	err      error
	Throwable
}

func NewUnableToReadDirectory(location string, err error) *UnableToReadDirectory {
	return &UnableToReadDirectory{
		location:  location,
		err:       err,
		Throwable: exception.New(fmt.Sprintf("Unable to read directory at location %s: %s", location, err)),
	}
}

func (err *UnableToReadDirectory) Unwrap() error {
	return err.err
}
