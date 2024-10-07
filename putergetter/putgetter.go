package putergetter

import (
	"context"

	blockstore "github.com/asabya/swarm-blockstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type PutGetter struct {
	tag             uint32
	api             blockstore.Client
	batch           string
	pin             bool
	redundancyLevel string
}

func NewPutGetter(api blockstore.Client, batch, redundancyLevel string, pin bool) (*PutGetter, error) {
	tag, err := api.CreateTag(swarm.ZeroAddress)
	if err != nil {
		return nil, err
	}
	return &PutGetter{
		tag:             tag,
		api:             api,
		batch:           batch,
		pin:             pin,
		redundancyLevel: redundancyLevel,
	}, nil
}

func (p *PutGetter) Get(ctx context.Context, address swarm.Address) (ch swarm.Chunk, err error) {
	return p.api.DownloadChunk(ctx, address)
}

func (p *PutGetter) Put(_ context.Context, ch swarm.Chunk) error {
	_, err := p.api.UploadChunk(p.tag, ch, p.batch, p.redundancyLevel, p.pin)
	if err != nil {
		return err
	}
	return nil
}
