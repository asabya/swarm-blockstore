package bee

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/asabya/swarm-blockstore/tar"

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
	pin        bool
}

type bytesPostResponse struct {
	Reference swarm.Address `json:"reference"`
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

func WithPinning(pin bool) Option {
	return func(c *Client) {
		c.pin = pin
	}
}

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

// CheckConnection is used to check if the bee client is up and running.
func (s *Client) CheckConnection() bool {
	// check if node is standalone bee
	matchString := "Ethereum Swarm Bee\n"
	data, _ := s.checkBee(false)
	if data == matchString {
		return true
	}

	// check if node is gateway-proxy
	data, err := s.checkBee(true)
	if err != nil {
		return false
	}
	matchString = "OK"
	s.isProxy = data == matchString

	return s.isProxy
}

func (s *Client) checkBee(isProxy bool) (string, error) {
	url := s.url
	if isProxy {
		url += healthUrl
	}
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return "", err
	}
	req.Close = true
	// skipcq: GO-S2307
	response, err := s.Do(req)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func socResource(owner, id, sig string) string {
	return fmt.Sprintf("/soc/%s/%s?sig=%s", owner, id, sig)
}

// UploadSOC is used construct and send a Single Owner Chunk to the Swarm bee client.
func (s *Client) UploadSOC(owner, id, signature, stamp, redundancyLevel string, pin bool, data []byte) (address swarm.Address, err error) {
	socResStr := socResource(owner, id, signature)
	fullUrl := fmt.Sprintf(s.url + socResStr)

	req, err := http.NewRequest(http.MethodPost, fullUrl, bytes.NewBuffer(data))
	if err != nil {
		return swarm.ZeroAddress, err
	}
	req.Close = true
	if stamp == "" {
		stamp = s.stamp
	}
	if redundancyLevel == "" {
		redundancyLevel = s.redundancy
	}
	req.Header.Set(SwarmPostageBatchId, stamp)
	req.Header.Set(contentTypeHeader, "application/octet-stream")
	req.Header.Set(swarmDeferredUploadHeader, "true")
	req.Header.Set(swarmErasureCodingHeader, redundancyLevel)
	if s.pin {
		pin = s.pin
	}
	if pin {
		req.Header.Set(swarmPinHeader, "true")
	}
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
	if s.pin {
		pin = s.pin
	}
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

// UploadBlob uploads a binary blob of data to Swarm network. It also optionally pins and encrypts the data.
func (s *Client) UploadBlob(tag uint32, stamp, redundancyLevel string, pin, encrypt bool, data io.Reader) (address swarm.Address, err error) {
	fullUrl := s.url + bytesUploadDownloadUrl
	req, err := http.NewRequest(http.MethodPost, fullUrl, data)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	req.Close = true
	if stamp == "" {
		stamp = s.stamp
	}
	if redundancyLevel == "" {
		redundancyLevel = s.redundancy
	}
	req.Header.Set(swarmPinHeader, fmt.Sprintf("%t", pin))
	req.Header.Set(swarmEncryptHeader, fmt.Sprintf("%t", encrypt))
	req.Header.Set(contentTypeHeader, "application/octet-stream")
	req.Header.Set(swarmErasureCodingHeader, redundancyLevel)

	if tag > 0 {
		req.Header.Set(swarmTagHeader, fmt.Sprintf("%d", tag))
	}
	req.Header.Set(SwarmPostageBatchId, stamp)
	req.Header.Set(swarmDeferredUploadHeader, "true")

	response, err := s.Do(req)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return swarm.ZeroAddress, errors.New("error uploading blob")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return swarm.ZeroAddress, errors.New(string(respData))
		}
		return swarm.ZeroAddress, errors.New(beeErr.Message)
	}

	var resp bytesPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("error unmarshalling response")
	}

	return resp.Reference, nil
}

// DownloadBlob downloads a blob of binary data from the Swarm network.
func (s *Client) DownloadBlob(address swarm.Address) (io.ReadCloser, int, error) {

	fullUrl := s.url + bytesUploadDownloadUrl + "/" + address.String()
	req, err := http.NewRequest(http.MethodGet, fullUrl, http.NoBody)
	if err != nil {
		return nil, http.StatusNotFound, err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	if response.StatusCode != http.StatusOK {
		respData, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, response.StatusCode, errors.New("error downloading blob")
		}

		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return nil, response.StatusCode, errors.New(string(respData))
		}
		return nil, response.StatusCode, errors.New(beeErr.Message)
	}

	return response.Body, response.StatusCode, nil
}

// UploadFileBzz uploads a file through bzz api
func (s *Client) UploadFileBzz(data []byte, fileName, stamp, redundancyLevel string, pin bool) (address swarm.Address, err error) {

	fullUrl := s.url + bzzUrl + "?name=" + fileName
	req, err := http.NewRequest(http.MethodPost, fullUrl, bytes.NewBuffer(data))
	if err != nil {
		return swarm.ZeroAddress, err
	}
	req.Close = true
	if stamp == "" {
		stamp = s.stamp
	}
	if redundancyLevel == "" {
		redundancyLevel = s.redundancy
	}
	req.Header.Set(swarmPinHeader, fmt.Sprintf("%t", pin))
	req.Header.Set(SwarmPostageBatchId, stamp)
	req.Header.Set(contentTypeHeader, "application/json")
	req.Header.Set(swarmErasureCodingHeader, redundancyLevel)

	response, err := s.Do(req)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	// skipcq: GO-S2307
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return swarm.ZeroAddress, errors.New("error downloading bzz")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return swarm.ZeroAddress, errors.New(string(respData))
		}
		return swarm.ZeroAddress, errors.New(beeErr.Message)
	}

	var resp bytesPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("error unmarshalling response")
	}
	return resp.Reference, nil
}

// UploadBzz uploads a tar through bzz api
func (s *Client) UploadBzz(data *tar.Stream, stamp, redundancyLevel string, pin bool) (address swarm.Address, err error) {

	fullUrl := s.url + bzzUrl
	req, err := http.NewRequest(http.MethodPost, fullUrl, data.Output())
	if err != nil {
		return swarm.ZeroAddress, err
	}
	req.Close = true

	if stamp == "" {
		stamp = s.stamp
	}
	if redundancyLevel == "" {
		redundancyLevel = s.redundancy
	}
	req.Header.Set(swarmPinHeader, fmt.Sprintf("%t", pin))
	req.Header.Set(SwarmPostageBatchId, stamp)
	req.Header.Set("Content-Type", "application/x-tar")
	req.Header.Set("Swarm-Collection", "true")
	req.Header.Set(swarmErasureCodingHeader, redundancyLevel)

	response, err := s.Do(req)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return swarm.ZeroAddress, errors.New("error downloading bzz")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return swarm.ZeroAddress, errors.New(string(respData))
		}
		return swarm.ZeroAddress, errors.New(beeErr.Message)
	}

	var resp bytesPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("error unmarshalling response")
	}
	return resp.Reference, nil
}

// DownloadBzz downloads bzz data from the Swarm network.
func (s *Client) DownloadBzz(address swarm.Address) ([]byte, int, error) {

	addrString := address.String()
	fullUrl := s.url + bzzUrl + "/" + addrString
	req, err := http.NewRequest(http.MethodGet, fullUrl, http.NoBody)
	if err != nil {
		return nil, http.StatusNotFound, err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return nil, http.StatusNotFound, err
	}
	// skipcq: GO-S2307
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, response.StatusCode, errors.New("error downloading bzz")
	}

	if response.StatusCode != http.StatusOK {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return nil, response.StatusCode, errors.New(string(respData))
		}
		return nil, response.StatusCode, errors.New(beeErr.Message)
	}
	return respData, response.StatusCode, nil
}

// DownloadFileBzz downloads file at bzz collection from the Swarm network.
func (s *Client) DownloadFileBzz(address swarm.Address, filename string) (io.ReadCloser, uint64, error) {

	fullUrl := s.url + filepath.ToSlash(filepath.Join(bzzUrl, address.String(), filename))
	req, err := http.NewRequest(http.MethodGet, fullUrl, http.NoBody)
	if err != nil {
		return nil, 0, err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if response.StatusCode != http.StatusOK {
		respData, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, 0, errors.New("error downloading bzz")
		}

		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return nil, 0, errors.New(string(respData))
		}
		return nil, 0, errors.New(beeErr.Message)
	}

	len, err := strconv.ParseUint(response.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, 0, err
	}

	return response.Body, len, nil
}

// DeleteReference unpins a reference so that it will be garbage collected by the Swarm network.
func (s *Client) DeleteReference(address swarm.Address) error {

	fullUrl := s.url + pinsUrl + address.String()
	req, err := http.NewRequest(http.MethodDelete, fullUrl, http.NoBody)
	if err != nil {
		return err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		respData, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to unpin reference : %s", respData)
	} else {
		_, _ = io.Copy(io.Discard, response.Body)
	}

	return nil
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

func (s *Client) CreateFeedManifest(owner, topic, stamp string, pin bool) (swarm.Address, error) {

	fullUrl := s.url + feedsUrl + owner + "/" + topic
	req, err := http.NewRequest(http.MethodPost, fullUrl, nil)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	req.Close = true
	if stamp == "" {
		stamp = s.stamp
	}
	req.Header.Set(SwarmPostageBatchId, stamp)
	if s.pin {
		pin = s.pin
	}
	if pin {
		req.Header.Set(swarmPinHeader, "true")
	}
	response, err := s.Do(req)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return swarm.ZeroAddress, errors.New("error create feed manifest")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return swarm.ZeroAddress, errors.New(string(respData))
		}
		return swarm.ZeroAddress, errors.New(beeErr.Message)
	}

	var resp bytesPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("error unmarshalling response")
	}

	return resp.Reference, nil
}

func (s *Client) GetLatestFeedManifest(owner, topic string) (swarm.Address, string, string, error) {

	fullUrl := s.url + feedsUrl + owner + "/" + topic

	req, err := http.NewRequest(http.MethodGet, fullUrl, nil)
	if err != nil {
		return swarm.ZeroAddress, "", "", err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return swarm.ZeroAddress, "", "", err
	}
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return swarm.ZeroAddress, "", "", errors.New("error getting latest feed manifest")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return swarm.ZeroAddress, "", "", errors.New(string(respData))
		}
		return swarm.ZeroAddress, "", "", errors.New(beeErr.Message)
	}

	var resp bytesPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return swarm.ZeroAddress, "", "", fmt.Errorf("error unmarshalling response")
	}

	return resp.Reference, response.Header.Get("swarm-feed-index"), response.Header.Get("swarm-feed-index-next"), nil
}

// GetTag gets sync status of a given tag
func (s *Client) GetTag(tag uint32) (int64, int64, int64, error) {
	// gateway proxy does not have tags api exposed
	if s.isProxy {
		return 0, 0, 0, nil
	}

	fullUrl := s.url + tagsUrl + fmt.Sprintf("/%d", tag)

	req, err := http.NewRequest(http.MethodGet, fullUrl, http.NoBody)
	if err != nil {
		return 0, 0, 0, err
	}
	req.Close = true

	response, err := s.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	// skipcq: GO-S2307
	defer response.Body.Close()

	respData, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, 0, 0, errors.New("error getting tag")
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var beeErr *beeError
		err = json.Unmarshal(respData, &beeErr)
		if err != nil {
			return 0, 0, 0, errors.New(string(respData))
		}
		return 0, 0, 0, errors.New(beeErr.Message)
	}

	var resp tagPostResponse
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error unmarshalling response")
	}

	return resp.Total, resp.Processed, resp.Synced, nil
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
