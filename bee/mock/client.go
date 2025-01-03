package mock

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/accesscontrol"

	"github.com/ethereum/go-ethereum/common"
	mockac "github.com/ethersphere/bee/v2/pkg/accesscontrol/mock"
	accountingmock "github.com/ethersphere/bee/v2/pkg/accounting/mock"
	"github.com/ethersphere/bee/v2/pkg/api"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/log"
	p2pmock "github.com/ethersphere/bee/v2/pkg/p2p/mock"
	"github.com/ethersphere/bee/v2/pkg/pingpong"
	"github.com/ethersphere/bee/v2/pkg/postage"
	mockbatchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	mockpost "github.com/ethersphere/bee/v2/pkg/postage/mock"
	"github.com/ethersphere/bee/v2/pkg/postage/postagecontract"
	"github.com/ethersphere/bee/v2/pkg/pss"
	"github.com/ethersphere/bee/v2/pkg/pusher"
	"github.com/ethersphere/bee/v2/pkg/resolver"
	resolverMock "github.com/ethersphere/bee/v2/pkg/resolver/mock"
	"github.com/ethersphere/bee/v2/pkg/settlement/pseudosettle"
	chequebookmock "github.com/ethersphere/bee/v2/pkg/settlement/swap/chequebook/mock"
	erc20mock "github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20/mock"
	swapmock "github.com/ethersphere/bee/v2/pkg/settlement/swap/mock"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/status"
	"github.com/ethersphere/bee/v2/pkg/steward"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemstore"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/staking"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
	topologymock "github.com/ethersphere/bee/v2/pkg/topology/mock"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

var (
	BatchOk    = make([]byte, 32)
	BatchOkStr string
)

// nolint:gochecknoinits
func init() {
	_, _ = rand.Read(BatchOk)

	BatchOkStr = hex.EncodeToString(BatchOk)
}

type TestServerOptions struct {
	Storer             api.Storer
	StateStorer        storage.StateStorer
	Resolver           resolver.Interface
	Pss                pss.Interface
	WsPath             string
	WsPingPeriod       time.Duration
	Logger             log.Logger
	PreventRedirect    bool
	Feeds              feeds.Factory
	CORSAllowedOrigins []string
	PostageContract    postagecontract.Interface
	StakingContract    staking.Contract
	Post               postage.Service
	AccessControl      accesscontrol.Controller
	Steward            steward.Interface
	WsHeaders          http.Header
	DirectUpload       bool
	Probe              *api.Probe

	Overlay         swarm.Address
	PublicKey       ecdsa.PublicKey
	PSSPublicKey    ecdsa.PublicKey
	EthereumAddress common.Address
	BlockTime       time.Duration
	P2P             *p2pmock.Service
	Pingpong        pingpong.Interface
	TopologyOpts    []topologymock.Option
	AccountingOpts  []accountingmock.Option
	ChequebookOpts  []chequebookmock.Option
	SwapOpts        []swapmock.Option
	TransactionOpts []transactionmock.Option

	BatchStore postage.Storer
	SyncStatus func() (bool, error)

	BackendOpts     []backendmock.Option
	Erc20Opts       []erc20mock.Option
	BeeMode         api.BeeNodeMode
	NodeStatus      *status.Service
	PinIntegrity    api.PinIntegrity
	WhitelistedAddr string
}

func NewTestBeeServer(t *testing.T, o TestServerOptions) string {
	t.Helper()
	pk, _ := crypto.GenerateSecp256k1Key()
	signer := crypto.NewDefaultSigner(pk)

	if o.Logger == nil {
		o.Logger = log.Noop
	}
	if o.Resolver == nil {
		o.Resolver = resolverMock.NewResolver()
	}
	if o.WsPingPeriod == 0 {
		o.WsPingPeriod = 60 * time.Second
	}
	if o.Post == nil {
		o.Post = mockpost.New()
	}
	if o.AccessControl == nil {
		o.AccessControl = mockac.New()
	}
	if o.BatchStore == nil {
		o.BatchStore = mockbatchstore.New(mockbatchstore.WithAcceptAllExistsFunc()) // default is with accept-all Exists() func
	}
	if o.SyncStatus == nil {
		o.SyncStatus = func() (bool, error) { return true, nil }
	}

	var chanStore *chanStorer

	topologyDriver := topologymock.NewTopologyDriver(o.TopologyOpts...)
	acc := accountingmock.NewAccounting(o.AccountingOpts...)
	settlement := swapmock.New(o.SwapOpts...)
	chequebook := chequebookmock.NewChequebook(o.ChequebookOpts...)
	ln := lightnode.NewContainer(o.Overlay)

	transaction := transactionmock.New(o.TransactionOpts...)

	storeRecipient := statestore.NewStateStore()
	recipient := pseudosettle.New(nil, o.Logger, storeRecipient, nil, big.NewInt(10000), big.NewInt(10000), o.P2P)

	if o.StateStorer == nil {
		o.StateStorer = storeRecipient
	}
	erc20 := erc20mock.New(o.Erc20Opts...)
	backend := backendmock.New(o.BackendOpts...)

	var extraOpts = api.ExtraOptions{
		TopologyDriver:  topologyDriver,
		Accounting:      acc,
		Pseudosettle:    recipient,
		LightNodes:      ln,
		Swap:            settlement,
		Chequebook:      chequebook,
		Pingpong:        o.Pingpong,
		BlockTime:       o.BlockTime,
		Storer:          o.Storer,
		Resolver:        o.Resolver,
		Pss:             o.Pss,
		FeedFactory:     o.Feeds,
		Post:            o.Post,
		AccessControl:   o.AccessControl,
		PostageContract: o.PostageContract,
		Steward:         o.Steward,
		SyncStatus:      o.SyncStatus,
		Staking:         o.StakingContract,
		NodeStatus:      o.NodeStatus,
		PinIntegrity:    o.PinIntegrity,
	}

	// By default bee mode is set to full mode.
	if o.BeeMode == api.UnknownMode {
		o.BeeMode = api.FullMode
	}

	o.CORSAllowedOrigins = append(o.CORSAllowedOrigins, "*")

	s := api.New(o.PublicKey, o.PSSPublicKey, o.EthereumAddress, []string{}, o.Logger, transaction, o.BatchStore, o.BeeMode, true, true, backend, o.CORSAllowedOrigins, inmemstore.New())
	testutil.CleanupCloser(t, s)

	s.SetP2P(o.P2P)

	s.SetSwarmAddress(&o.Overlay)
	s.SetProbe(o.Probe)

	noOpTracer, tracerCloser, _ := tracing.NewTracer(&tracing.Options{
		Enabled: false,
	})
	testutil.CleanupCloser(t, tracerCloser)

	s.Configure(signer, noOpTracer, api.Options{
		CORSAllowedOrigins: o.CORSAllowedOrigins,
		WsPingPeriod:       o.WsPingPeriod,
	}, extraOpts, 1, erc20)

	s.MountTechnicalDebug()
	s.MountDebug()
	s.MountAPI()

	if o.DirectUpload {
		chanStore = newChanStore(o.Storer.PusherFeed())
		t.Cleanup(chanStore.stop)
	}

	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return ts.URL
}

type contractCall int

func (c contractCall) String() string {
	switch c {
	case isWinnerCall:
		return "isWinnerCall"
	case revealCall:
		return "revealCall"
	case commitCall:
		return "commitCall"
	case claimCall:
		return "claimCall"
	}
	return "unknown"
}

const (
	isWinnerCall contractCall = iota
	revealCall
	commitCall
	claimCall
)

type mockContract struct {
	callsList []contractCall
	mtx       sync.Mutex
}

func (m *mockContract) Fee(ctx context.Context, txHash common.Hash) *big.Int {
	return big.NewInt(1000)
}

func (m *mockContract) ReserveSalt(context.Context) ([]byte, error) {
	return nil, nil
}

func (m *mockContract) IsPlaying(context.Context, uint8) (bool, error) {
	return true, nil
}

func (m *mockContract) IsWinner(context.Context) (bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, isWinnerCall)
	return false, nil
}

func (m *mockContract) Claim(context.Context, redistribution.ChunkInclusionProofs) (common.Hash, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, claimCall)
	return common.Hash{}, nil
}

func (m *mockContract) Commit(context.Context, []byte, uint64) (common.Hash, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, commitCall)
	return common.Hash{}, nil
}

func (m *mockContract) Reveal(context.Context, uint8, []byte, []byte) (common.Hash, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, revealCall)
	return common.Hash{}, nil
}

type mockHealth struct{}

func (m *mockHealth) IsHealthy() bool { return true }

type chanStorer struct {
	lock   sync.Mutex
	chunks map[string]struct{}
	quit   chan struct{}
}

func newChanStore(cc <-chan *pusher.Op) *chanStorer {
	c := &chanStorer{
		chunks: make(map[string]struct{}),
		quit:   make(chan struct{}),
	}
	go c.drain(cc)
	return c
}

func (c *chanStorer) drain(cc <-chan *pusher.Op) {
	for {
		select {
		case op := <-cc:
			c.lock.Lock()
			c.chunks[op.Chunk.Address().ByteString()] = struct{}{}
			c.lock.Unlock()
			op.Err <- nil
		case <-c.quit:
			return
		}
	}
}

func (c *chanStorer) stop() {
	close(c.quit)
}

func (c *chanStorer) Has(addr swarm.Address) bool {
	c.lock.Lock()
	_, ok := c.chunks[addr.ByteString()]
	c.lock.Unlock()

	return ok
}
