package s3_test

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

func rangeChecksumFolder(t *testing.T, payload []byte, valid bool) (*walgs3.Folder, *atomic.Int32) {
	t.Helper()
	requests := new(atomic.Int32)
	checksum := make([]byte, 4)
	binary.BigEndian.PutUint32(checksum, crc32.ChecksumIEEE(payload))
	if !valid {
		checksum[0] ^= 1
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("X-Amz-Checksum-Crc32", base64.StdEncoding.EncodeToString(checksum))
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)
	api := awss3.NewFromConfig(aws.Config{
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("test-access", "test-secret", ""),
		HTTPClient:                 server.Client(),
		Retryer:                    func() aws.Retryer { return aws.NopRetryer{} },
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenSupported,
	}, func(o *awss3.Options) { o.BaseEndpoint = aws.String(server.URL); o.UsePathStyle = true })
	return walgs3.NewFolder(api, nil, "", &walgs3.Config{
		Bucket: "test", RangeBatchEnabled: true, RangeMaxRetries: 2,
	}), requests
}

func TestRangeReaderSDKChecksum(t *testing.T) {
	for _, size := range []int{0, 1, 4096, 32768, 36864, 100003} {
		for _, valid := range []bool{true, false} {
			t.Run(fmt.Sprintf("size=%d/valid=%t", size, valid), func(t *testing.T) {
				payload := bytes.Repeat([]byte("x"), size)
				folder, requests := rangeChecksumFolder(t, payload, valid)
				reader, err := folder.ReadObject(t.Context(), "object")
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				actual, err := io.ReadAll(reader)
				if valid {
					if err != nil || !bytes.Equal(actual, payload) {
						t.Fatalf("healthy n=%d err=%v", len(actual), err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "checksum did not match") {
					t.Fatalf("checksum error lost: n=%d err=%v", len(actual), err)
				}
				if got := requests.Load(); got != 1 {
					t.Fatalf("unexpected GET count %d", got)
				}
				t.Logf("n=%d err=%v", len(actual), err)
			})
		}
	}
}

// Read-ahead may detect a checksum failure even when the caller only wants a prefix.
func TestRangeReaderPrefixPropagatesChecksumError(t *testing.T) {
	folder, requests := rangeChecksumFolder(t, bytes.Repeat([]byte("x"), 256), false)
	reader, err := folder.ReadObject(t.Context(), "object")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	p := make([]byte, 1)
	n, err := io.ReadFull(reader, p)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "checksum did not match") {
		t.Fatalf("expected withheld prefix and checksum error, n=%d err=%v", n, err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("unexpected GET count %d", got)
	}
}
