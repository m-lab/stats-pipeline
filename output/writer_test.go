package output

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/m-lab/go/cloudtest/gcsfake"
	"github.com/m-lab/go/testingx"
	"github.com/m-lab/go/uploader"
)

func TestGCSWriter_Write(t *testing.T) {
	failingBucket := gcsfake.NewBucketHandle()
	failingBucket.WritesMustFail = true

	client := &gcsfake.GCSClient{}
	client.AddTestBucket("test_bucket", gcsfake.NewBucketHandle())
	client.AddTestBucket("failing_bucket", failingBucket)

	tests := []struct {
		name    string
		path    string
		content []byte
		wantErr bool
	}{
		{
			name:    "success-write",
			path:    "output/name",
			content: []byte{0, 1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := NewGCSWriter(uploader.New(client, "test_bucket"))
			if err := u.Write(context.Background(), tt.path, tt.content); (err != nil) != tt.wantErr {
				t.Errorf("GCSWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLocalWriter_Write(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		path    string
		content []byte
		wantErr bool
	}{
		{
			name:    "success",
			dir:     t.TempDir(),
			path:    "output/name",
			content: []byte{0, 1, 2},
		},
		{
			name:    "error",
			dir:     t.TempDir(),
			path:    "file.not-a-dir/name",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				p := filepath.Join(tt.dir, tt.path)
				err := os.MkdirAll(filepath.Dir(filepath.Dir(p)), os.ModePerm)
				testingx.Must(t, err, "failed to mkdir")
				// create a file where a directory should be.
				f, err := os.Create(filepath.Dir(p))
				testingx.Must(t, err, "failed to create file")
				f.Close()
			}
			lu := NewLocalWriter(context.Background(), tt.dir)
			if err := lu.Write(context.Background(), tt.path, tt.content); (err != nil) != tt.wantErr {
				t.Errorf("LocalWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
