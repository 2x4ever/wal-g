package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

type rangeHTTPRoundTripper struct {
	http.RoundTripper
	wrapBody func(io.ReadCloser) io.ReadCloser
}

func (r rangeHTTPRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := r.RoundTripper.RoundTrip(req)
	if err == nil && response.Header.Get("X-Test-Truncated") == "true" {
		response.Body = r.wrapBody(response.Body)
	}
	return response, err
}

type rangeHTTPObservedBody struct {
	io.ReadCloser
	onRead func(int, error)
}

func (b rangeHTTPObservedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.onRead(n, err)
	return n, err
}

// Exercise the real net/http body and AWS SDK, including junk delivered in
// successful reads before a later unexpected EOF. The server waits until the
// client has read all injected bytes before closing, so TCP read boundaries
// cannot hide the regression by returning the junk alongside the read error.
func TestRangeReaderHTTPRollbackDiscardsInjectedErrorText(t *testing.T) {
	const (
		objectSize = 32 * 1024
		prefixSize = 12 * 1024
		junkSize   = 300
	)
	original := make([]byte, objectSize)
	for i := range original {
		original[i] = byte((i*37 + i/251) % 256)
	}
	junk := bytes.Repeat([]byte("server-error!"), junkSize/13+1)[:junkSize]
	partialBody := append(append([]byte(nil), original[:prefixSize]...), junk...)
	// 12288 valid bytes + 300 injected bytes, minus the 4096-byte holdback.
	wantRange := "bytes=8492-"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	allPartialBytesRead := make(chan struct{})
	var readSignal sync.Once
	var mu sync.Mutex
	var ranges []string
	var acceptedBytes int
	var bodyReadError error

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestIndex := len(ranges)
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		if r.Method != http.MethodGet || r.URL.Path != "/test-bucket/base/object" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if requestIndex == 0 {
			connection, buffered, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack first response: %v", err)
				return
			}
			defer connection.Close()
			_, err = fmt.Fprintf(buffered, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nETag: \"test-object\"\r\nX-Test-Truncated: true\r\nConnection: close\r\n\r\n", objectSize)
			if err == nil {
				_, err = buffered.Write(partialBody)
			}
			if err == nil {
				err = buffered.Flush()
			}
			if err != nil {
				t.Errorf("write truncated response: %v", err)
				return
			}
			select {
			case <-allPartialBytesRead:
				// Close with fewer bytes than Content-Length: net/http must
				// report unexpected EOF on a subsequent body read.
			case <-ctx.Done():
				t.Errorf("client did not consume injected bytes: %v", ctx.Err())
			}
			return
		}

		if got := r.Header.Get("If-Match"); got != `"test-object"` {
			t.Errorf("resumed GET did not preserve ETag guard: %q", got)
		}
		rangeHeader := r.Header.Get("Range")
		if !strings.HasPrefix(rangeHeader, "bytes=") || !strings.HasSuffix(rangeHeader, "-") {
			http.Error(w, "missing open-ended Range", http.StatusBadRequest)
			return
		}
		start, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(rangeHeader, "bytes="), "-"))
		if err != nil || start < 0 || start >= len(original) {
			http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("ETag", `"test-object"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(original)-start))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(original)-1, len(original)))
		w.WriteHeader(http.StatusPartialContent)
		if _, err = w.Write(original[start:]); err != nil {
			t.Errorf("write resumed response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	httpClient := &http.Client{Transport: rangeHTTPRoundTripper{
		RoundTripper: transport,
		wrapBody: func(body io.ReadCloser) io.ReadCloser {
			return rangeHTTPObservedBody{ReadCloser: body, onRead: func(n int, err error) {
				mu.Lock()
				if err == nil {
					acceptedBytes += n
				} else {
					bodyReadError = err
				}
				readAll := acceptedBytes == len(partialBody)
				mu.Unlock()
				if readAll {
					readSignal.Do(func() { close(allPartialBytesRead) })
				}
			}}
		},
	}}
	api := awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test-access", "test-secret", ""),
		HTTPClient:  httpClient,
		// Test range recovery independently of the SDK's request retries.
		Retryer: func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
	uploader := walgs3.NewUploader(nil, "", "", "", "STANDARD", "", -1)
	folder := walgs3.NewFolder(api, uploader, "base/", &walgs3.Config{
		Bucket:            "test-bucket",
		RangeBatchEnabled: true,
		RangeMaxRetries:   1,
	})
	reader, err := folder.ReadObject(ctx, "object")
	require.NoError(t, err)
	defer reader.Close()
	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, actual),
		"HTTP error text must not reach the returned object (expected %d bytes, got %d)", len(original), len(actual))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, len(partialBody), acceptedBytes, "junk must have arrived in successful HTTP body reads")
	require.True(t, errors.Is(bodyReadError, io.ErrUnexpectedEOF), "body read error: %v", bodyReadError)
	require.Equal(t, []string{"", wantRange}, ranges, "initial GET and one GET rolled back by 4096 bytes")
}
