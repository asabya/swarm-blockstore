package blockstore

import (
	"context"
	"io"

	"github.com/asabya/swarm-blockstore/tar"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Client is the interface for block store
type Client interface {
	CheckConnection() bool
	UploadSOC(owner, id, signature, stamp, redundancyLevel string, pin bool, data []byte) (address swarm.Address, err error)
	UploadChunk(tag uint32, ch swarm.Chunk, stamp, redundancyLevel string, pin bool) (address swarm.Address, err error)
	UploadBlob(tag uint32, stamp, redundancyLevel string, pin, encrypt bool, data io.Reader) (address swarm.Address, err error)
	UploadFileBzz(data []byte, fileName, stamp, redundancyLevel string, pin bool) (address swarm.Address, err error)
	UploadBzz(data *tar.Stream, stamp, redundancyLevel string, pin bool) (address swarm.Address, err error)
	DownloadChunk(ctx context.Context, address swarm.Address) (chunk swarm.Chunk, err error)
	DownloadBlob(address swarm.Address) (data io.ReadCloser, respCode int, err error)
	DownloadBzz(address swarm.Address) ([]byte, int, error)
	DownloadFileBzz(address swarm.Address, filename string) (data io.ReadCloser, contentLength uint64, err error)
	DeleteReference(address swarm.Address) error
	CreateTag(address swarm.Address) (uint32, error)
	GetTag(tag uint32) (int64, int64, int64, error)
	CreateFeedManifest(owner, topic, stamp string, pin bool) (address swarm.Address, err error)
	GetLatestFeedManifest(owner, topic string) (address swarm.Address, index, nextIndex string, err error)
}
