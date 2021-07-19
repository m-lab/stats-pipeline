package output

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/m-lab/go/uploader"
)

// GCSWriter provides Write operations to a GCS bucket.
type GCSWriter struct {
	up *uploader.Uploader
}

// NewGCSWriter creates a new GCSWriter from the given uploader.Uploader.
func NewGCSWriter(up *uploader.Uploader) *GCSWriter {
	return &GCSWriter{up: up}
}

// Write creates a new object at path containing content.
func (u *GCSWriter) Write(ctx context.Context, path string, content []byte) error {
	_, err := u.up.Upload(ctx, path, content)
	return err
}

// LocalWriter provides Write operations to a local directory.
type LocalWriter struct {
	dir string
}

// NewLocalWriter creates a new LocalWriter for the given output directory.
func NewLocalWriter(dir string) *LocalWriter {
	return &LocalWriter{dir: dir}
}

// Write creates a new file at path containing content.
func (lu *LocalWriter) Write(ctx context.Context, path string, content []byte) error {
	p := filepath.Join(lu.dir, path)
	d := filepath.Dir(p) // path may include additional directory elements.
	err := os.MkdirAll(d, os.ModePerm)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(p, content, 0664)
}
