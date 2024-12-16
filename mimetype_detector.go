package filesystem

import (
	"io"
	"mime"
	"path/filepath"

	"github.com/gabriel-vasile/mimetype"
)

const defaultMimeType = "application/octet-stream"

type MimeTypeDetector struct{}

func NewMimeTypeDetector() *MimeTypeDetector {
	return &MimeTypeDetector{}
}

func (m *MimeTypeDetector) Detect(location string, content []byte) string {
	mt := m.DetectFromContent(content)
	if mt == defaultMimeType {
		goto fallback
	} else {
		return mt
	}
fallback:
	mt2 := m.DetectFromPath(location)
	return mt2
}

func (m *MimeTypeDetector) DetectFromPath(location string) string {
	ext := filepath.Ext(location)
	mt := mime.TypeByExtension(ext)
	if mt == "" {
		return defaultMimeType
	}
	return mt
}

func (m *MimeTypeDetector) DetectFromContent(content []byte) string {
	return mimetype.Detect(content).String()
}

func (m *MimeTypeDetector) DetectFromFile(location string) string {
	mt, err := mimetype.DetectFile(location)
	if err != nil {
		return defaultMimeType
	}
	return mt.String()
}

func (m *MimeTypeDetector) DetectFromReader(reader io.Reader) string {
	mt, err := mimetype.DetectReader(reader)
	if err != nil {
		return defaultMimeType
	}
	return mt.String()
}
