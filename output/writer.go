package output

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

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
	dir  string
	c    *sync.Cond
	safe bool
}

// NewLocalWriter creates a new LocalWriter for the given output directory.
func NewLocalWriter(ctx context.Context, dir string) *LocalWriter {
	lu := &LocalWriter{dir: dir, c: sync.NewCond(&sync.Mutex{}), safe: true}
	go lu.monitorDir(ctx)
	return lu
}

// monitorDir is meant to run as a goroutine in the background to gate writes to
// the monitored output directory.
func (lu *LocalWriter) monitorDir(ctx context.Context) {
	for ctx.Err() == nil {
		time.Sleep(time.Second)

		stat := syscall.Statfs_t{}
		err := syscall.Statfs(lu.dir, &stat)
		if err != nil {
			log.Printf("Reading statsfs failed with error: %v", err)
			// Abort. The system is in a bad state.
			return
		}

		lu.c.L.Lock()
		if float64(stat.Ffree)/float64(stat.Files) < 0.1 || float64(stat.Bfree)/float64(stat.Blocks) < 0.1 {
			// Not safe to write.
			lu.safe = false
		} else {
			// Safe to write.
			lu.safe = true
			lu.c.Broadcast()
		}
		lu.c.L.Unlock()
	}
}

func (lu *LocalWriter) waitUntilSafeToWrite() {
	lu.c.L.Lock()
	for !lu.safe {
		lu.c.Wait()
	}
	lu.c.L.Unlock()
}

// Write creates a new file at path containing content.
func (lu *LocalWriter) Write(ctx context.Context, path string, content []byte) error {
	p := filepath.Join(lu.dir, path)
	d := filepath.Dir(p) // path may include additional directory elements.
	err := os.MkdirAll(d, os.ModePerm)
	if err != nil {
		return err
	}
	lu.waitUntilSafeToWrite()
	return ioutil.WriteFile(p, content, 0664)
}
