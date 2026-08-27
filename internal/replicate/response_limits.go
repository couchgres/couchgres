package replicate

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

var (
	// ErrResponseTooLarge reports a replication peer response exceeding the
	// fixed in-memory response ceiling.
	ErrResponseTooLarge = errors.New("replication response exceeds size limit")
	// ErrAttachmentTooLarge reports one multipart attachment exceeding its
	// fixed in-memory ceiling.
	ErrAttachmentTooLarge = errors.New("replication attachment exceeds size limit")
)

func readResponseBody(resp *http.Response, limit int64) ([]byte, error) {
	reader, err := boundedResponseBody(resp, limit)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func boundedResponseBody(resp *http.Response, limit int64) (io.Reader, error) {
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, limit)
	}
	return &maxBytesReader{source: resp.Body, remaining: limit, limitErr: ErrResponseTooLarge}, nil
}

func readAllBounded(reader io.Reader, limit int64, limitErr error) ([]byte, error) {
	return io.ReadAll(&maxBytesReader{source: reader, remaining: limit, limitErr: limitErr})
}

func drainResponseBody(resp *http.Response, limit int64) error {
	reader, err := boundedResponseBody(resp, limit)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, reader)
	return err
}

// maxBytesReader permits exactly remaining bytes. Once the budget is spent it
// probes for EOF so an exact-sized response succeeds while one extra byte
// returns the configured limit error without being retained in memory.
type maxBytesReader struct {
	source    io.Reader
	remaining int64
	limitErr  error
}

func (r *maxBytesReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining > 0 {
		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
		n, err := r.source.Read(p)
		r.remaining -= int64(n)
		return n, err
	}
	var probe [1]byte
	n, err := r.source.Read(probe[:])
	if n > 0 {
		return 0, r.limitErr
	}
	return 0, err
}
