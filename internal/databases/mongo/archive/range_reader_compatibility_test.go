package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	wals3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoRangeAPI struct {
	wals3.API
	get func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error)
}

func (a *mongoRangeAPI) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return a.get(in)
}

type mongoRangeBody struct {
	data    []byte
	chunk   int
	end     error
	withEOF bool
	closed  bool
}

func (b *mongoRangeBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, b.end
	}
	n := copy(p, b.data[:min(len(b.data), b.chunk)])
	b.data = b.data[n:]
	if len(b.data) == 0 && b.withEOF {
		return n, b.end
	}
	return n, nil
}
func (b *mongoRangeBody) Close() error { b.closed = true; return nil }

type mongoRangePipeWriter struct{ *io.PipeWriter }

func (p mongoRangePipeWriter) Close() error { return nil }

func mongoRangeDocuments(t *testing.T) ([]byte, [][]byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(7))
	var full []byte
	var records [][]byte
	for i := 0; i < 11; i++ {
		data := make([]byte, 7001+i*11)
		_, err := rng.Read(data)
		require.NoError(t, err)
		raw, err := bson.Marshal(bson.D{
			{Key: "ts", Value: bson.Timestamp{T: 1700000000, I: uint32(i + 1)}},
			{Key: "op", Value: "i"},
			{Key: "ns", Value: "test.records"},
			{Key: "o", Value: bson.D{
				{Key: "_id", Value: i},
				{Key: "data", Value: bson.Binary{Data: data}},
			}},
		})
		require.NoError(t, err)
		records = append(records, raw)
		full = append(full, raw...)
	}
	return full, records
}

func TestStorageDownloaderOplogArchiveRangeReader(t *testing.T) {
	for _, codec := range []string{"none", "gzip", "lz4", "lzma", "zstd"} {
		for _, chunk := range []int{1, 7, 4097, 32768} {
			for _, endWithBytes := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/chunk%d/finalBytesEOF%t", codec, chunk, endWithBytes), func(t *testing.T) {
					mongoRangeOplogCase(t, codec, chunk, endWithBytes, false)
				})
			}
		}
	}
}
func TestStorageDownloaderOplogArchiveRangeReaderRetry(t *testing.T) {
	for _, codec := range []string{"none", "gzip", "lz4", "lzma", "zstd"} {
		for _, endWithBytes := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/finalBytesError%t", codec, endWithBytes), func(t *testing.T) {
				mongoRangeOplogCase(t, codec, 997, endWithBytes, true)
			})
		}
	}
}

func mongoRangeOplogCase(t *testing.T, codec string, chunk int, endWithBytes, fail bool) {
	t.Helper()
	raw, records := mongoRangeDocuments(t)
	var encoded bytes.Buffer
	var writer io.WriteCloser
	var ext string
	if codec == "gzip" {
		writer = gzip.NewWriter(&encoded)
		ext = "gz"
	} else {
		comp, ok := compression.Compressors[codec]
		require.True(t, ok)
		writer = comp.NewWriter(&encoded)
		ext = comp.FileExtension()
	}
	if compression.FindDecompressor(ext) == nil {
		t.Skipf("decompressor for %s is unavailable in this build", ext)
	}
	_, err := writer.Write(raw)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	object := encoded.Bytes()
	require.Greater(t, len(object), 10000)
	arch, err := models.NewArchive(models.Timestamp{TS: 1700000000, Inc: 1}, models.Timestamp{TS: 1700000000, Inc: 11}, ext, models.ArchiveTypeOplog)
	require.NoError(t, err)
	cut := len(object) / 2
	var calls int
	var bodies []*mongoRangeBody
	api := &mongoRangeAPI{get: func(in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
		calls++
		if key := aws.ToString(in.Key); key != arch.Filename() {
			return nil, fmt.Errorf("unexpected object key %q, want %q", key, arch.Filename())
		}
		start := 0
		if calls > 1 {
			if _, err := fmt.Sscanf(aws.ToString(in.Range), "bytes=%d-", &start); err != nil {
				return nil, fmt.Errorf("invalid retry range %q: %w", aws.ToString(in.Range), err)
			}
			if start < 0 || start >= len(object) {
				return nil, fmt.Errorf("retry offset %d outside object of %d bytes", start, len(object))
			}
		}
		data := object[start:]
		end := io.EOF
		if calls == 1 && fail {
			data = append(append([]byte(nil), object[:cut]...), bytes.Repeat([]byte("S3_ERROR!!"), 30)...)
			end = io.ErrUnexpectedEOF
		}
		body := &mongoRangeBody{data: data, chunk: chunk, end: end, withEOF: endWithBytes}
		bodies = append(bodies, body)
		out := &awss3.GetObjectOutput{Body: body, ContentLength: aws.Int64(int64(len(object) - start)), ETag: aws.String("test-immutable")}
		if calls > 1 {
			out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, len(object)-1, len(object)))
		}
		return out, nil
	}}
	folder := wals3.NewFolder(api, nil, "", &wals3.Config{Bucket: "test", RangeBatchEnabled: true, RangeMaxRetries: 2})
	downloader := &StorageDownloader{oplogsFolder: folder}
	ctx, cancel := context.WithCancel(t.Context())
	pr, pw := io.Pipe()
	done := make(chan struct{})
	var downloadErr error
	go func() {
		defer close(done)
		downloadErr = downloader.DownloadOplogArchive(ctx, arch, mongoRangePipeWriter{pw})
		_ = pw.CloseWithError(downloadErr)
	}()
	t.Cleanup(func() {
		// Failed assertions must unblock and join the downloader before the next test.
		cancel()
		_ = pr.Close()
		_ = pw.Close()
		<-done
	})
	buffered := bufio.NewReaderSize(pr, 4*1024*1024) // same as StorageFetcher.FetchBetween
	var decoded []byte
	var gotRecords int
	for {
		doc, err := bson.ReadDocument(buffered)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "BSON read %d", gotRecords)
		if gotRecords >= len(records) {
			t.Fatal("unexpected extra BSON record")
		}
		require.True(t, bytes.Equal(records[gotRecords], []byte(doc)), "BSON record %d changed", gotRecords)
		op, err := models.OplogFromRaw(doc)
		require.NoError(t, err)
		require.Equal(t, models.Timestamp{TS: 1700000000, Inc: uint32(gotRecords + 1)}, op.TS)
		models.PutOplogEntry(op)
		decoded = append(decoded, doc...)
		gotRecords++
	}
	<-done
	require.NoError(t, downloadErr)
	require.Equal(t, len(records), gotRecords)
	require.True(t, bytes.Equal(raw, decoded), "decoded oplog bytes changed")
	expectedCalls := 1
	if fail {
		expectedCalls = 2
	}
	require.Equal(t, expectedCalls, calls)
	for _, body := range bodies {
		require.True(t, body.closed)
	}
	t.Logf("codec=%s BSON_records=%d source_bytes=%d decompressed_bytes=%d S3_GETs=%d", codec, gotRecords, len(object), len(decoded), calls)
}
