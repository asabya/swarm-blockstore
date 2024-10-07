package bee

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

const (
	maxIdleConnections        = 20
	maxConnectionsPerHost     = 256
	requestTimeout            = 6000
	healthUrl                 = "/health"
	chunkUploadDownloadUrl    = "/chunks"
	bytesUploadDownloadUrl    = "/bytes"
	bzzUrl                    = "/bzz"
	tagsUrl                   = "/tags"
	pinsUrl                   = "/pins/"
	feedsUrl                  = "/feeds/"
	swarmPinHeader            = "Swarm-Pin"
	swarmEncryptHeader        = "Swarm-Encrypt"
	SwarmPostageBatchId       = "Swarm-Postage-Batch-Id"
	swarmDeferredUploadHeader = "Swarm-Deferred-Upload"
	swarmErasureCodingHeader  = "Swarm-Redundancy-Level"
	swarmTagHeader            = "Swarm-Tag"
	contentTypeHeader         = "Content-Type"
)

// Client is a bee http client that satisfies blockstore.Client
type Client struct {
	url        string
	client     *http.Client
	isProxy    bool
	stamp      string
	redundancy string
}

type tagPostRequest struct {
	Address string `json:"address"`
}

type tagPostResponse struct {
	UID       uint32    `json:"uid"`
	StartedAt time.Time `json:"startedAt"`
	Total     int64     `json:"total"`
	Processed int64     `json:"processed"`
	Synced    int64     `json:"synced"`
}

type beeError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Option func(client *Client)

func WithStamp(stamp string) Option {
	return func(c *Client) {
		c.stamp = stamp
	}
}

func WithRedundancy(level string) Option {
	return func(c *Client) {
		c.redundancy = level
	}
}

// NewBeeClient creates a new client which connects to the Swarm bee node to access the Swarm network.
func NewBeeClient(apiUrl string, opts ...Option) *Client {
	c := &Client{
		url:    apiUrl,
		client: createHTTPClient(),
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

type chunkAddressResponse struct {
	Reference swarm.Address `json:"reference"`
}

// Do dispatches the HTTP request to the network
func (s *Client) Do(req *http.Request) (*http.Response, error) {
	return s.client.Do(req)
}

// UploadChunk uploads a chunk to Swarm network.
func (s *Client) UploadChunk(tag uint32, ch swarm.Chunk, stamp, redundancyLevel string, pin bool) (address swarm.Address, err error) {
	fullUrl := fmt.Sprintf(s.url + chunkUploadDownloadUrl)
	ctx := context.Background()
	ctx = redundancy.SetLevelInContext(ctx, redundancy.NONE)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullUrl, bytes.NewBuffer(ch.Data()))
	if err != nil {
		return swarm.ZeroAddress, err
	}
	if stamp == "" {
		stamp = s.stamp
	}
	if redundancyLevel == "" {
		redundancyLevel = s.redundancy
	}

	req.Header.Set(contentTypeHeader, "application/octet-stream")
	req.Header.Set(SwarmPostageBatchId, stamp)
	req.Header.Set(swarmDeferredUploadHeader, "true")
	req.Header.Set(swarmErasureCodingHeader, redundancyLevel)
	req.Header.Set(swarmTagHeader, fmt.Sprintf("%d", tag))

	if pin {
		req.Header.Set(swarmPinHeader, "true")
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	defer response.Body.Close()

	addrData, err := io.ReadAll(response.Body)
	if err != nil {
		return swarm.ZeroAddress, errors.New("error uploading data")
	}

	if response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(addrData, &beeErr)
		if err != nil {
			return swarm.ZeroAddress, errors.New(string(addrData))
		}
		return swarm.ZeroAddress, errors.New(beeErr.Message)
	}

	var addrResp *chunkAddressResponse
	err = json.Unmarshal(addrData, &addrResp)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	return addrResp.Reference, nil
}

// DownloadChunk downloads a chunk with given address from the Swarm network
func (s *Client) DownloadChunk(ctx context.Context, address swarm.Address) (chunk swarm.Chunk, err error) {
	path := chunkUploadDownloadUrl + "/" + address.String()
	fullUrl := fmt.Sprintf(s.url + path)
	req, err := http.NewRequest(http.MethodGet, fullUrl, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Close = true

	req = req.WithContext(ctx)

	response, err := s.Do(req)
	if err != nil {
		return nil, err
	}
	// skipcq: GO-S2307
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.New("error downloading data")
	}

	chunkData, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, errors.New("error downloading data")
	}

	return swarm.NewChunk(address, chunkData), nil
}

// CreateTag creates a tag for given address
func (s *Client) CreateTag(address swarm.Address) (uint32, error) {
	// gateway proxy does not have tags api exposed
	if s.isProxy {
		return 0, nil
	}

	fullUrl := s.url + tagsUrl
	var data []byte
	var err error
	if !address.IsZero() && !address.IsEmpty() {
		addrString := address.String()
		b := &tagPostRequest{Address: addrString}
		data, err = json.Marshal(b)
		if err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequest(http.MethodPost, fullUrl, bytes.NewBuffer(data))
	if err != nil {
		return 0, err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return 0, err
	}
	// skipcq: GO-S2307
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, errors.New("error create tag")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return 0, errors.New(string(respData))
		}
		return 0, errors.New(beeErr.Message)
	}

	var resp tagPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return 0, fmt.Errorf("error unmarshalling response")
	}

	return resp.UID, nil
}

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Timeout: time.Second * requestTimeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConnections,
			MaxConnsPerHost:     maxConnectionsPerHost,
		},
	}
	return client
}
