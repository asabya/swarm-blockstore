package tar

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"time"
)

type CollectionItem struct {
	Path string
	Size int64
	File io.ReadCloser
}

type Collection struct {
	Items []CollectionItem
}

// Stream is a tar stream writer
type Stream struct {
	buf *bytes.Buffer
	w   *tar.Writer
}

// NewStream creates a new TarStream instance
func NewStream() *Stream {
	buf := new(bytes.Buffer)
	return &Stream{
		buf: buf,
		w:   tar.NewWriter(buf),
	}
}

// BeginFile starts a new file in the tar archive
func (ts *Stream) BeginFile(item CollectionItem) error {
	hdr := &tar.Header{
		Name:    item.Path,
		Mode:    0777,
		Size:    item.Size,
		ModTime: time.Now(),
	}
	return ts.w.WriteHeader(hdr)
}

// AppendFile appends data to the current file in the tar archive
func (ts *Stream) AppendFile(data []byte) error {
	_, err := ts.w.Write(data)
	return err
}

// EndFile completes the current file entry
func (ts *Stream) EndFile() error {
	return nil // No padding needed as handled by tar.Writer
}

// End finishes the tar archive
func (ts *Stream) End() error {
	return ts.w.Close()
}

// Output returns the bytes buffer of the tar stream
func (ts *Stream) Output() *bytes.Buffer {
	return ts.buf
}

// GetWriter returns the tar writer
func (ts *Stream) GetWriter() *tar.Writer {
	return ts.w
}

func (ts *Stream) WriteItem(item CollectionItem) error {
	err := ts.BeginFile(item)
	if err != nil {
		return err
	}
	if item.File != nil {
		defer item.File.Close()

		_, err = io.CopyBuffer(ts.GetWriter(), item.File, make([]byte, 32*1024))
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("invalid collection item")
	}
	return ts.EndFile()
}
